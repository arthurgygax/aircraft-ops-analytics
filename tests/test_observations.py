"""The canonical observation table: decode, dedup, clean, segment.

The dedup tests are the important ones. They pin the *winner rule*, not just
the row count: a deduplication that keeps the wrong reception is still
deterministic and still wrong.
"""

import gzip
import json
from datetime import date, datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark", reason="Spark tests run in the spark container")

from adsb.observations import (  # noqa: E402
    GAP_SECONDS,
    assign_flights,
    decode,
    read_observations,
    to_observations,
    write_observations,
)
from adsb.spark_explore import read_aircraft  # noqa: E402

START = datetime(2025, 12, 30, 8, 0, 0)
RELEASE_DATE = date(2025, 12, 30)
DAY_START = 1767052800.0  # 2025-12-30T00:00:00Z, the fixture's day epoch


def point(offset, lat=47.4, lon=8.5, alt=10000, gs=250.0, track=90.0, vrate=64,
          details=None, source="adsb_icao"):
    """One raw trace point, in the positional layout readsb writes."""
    return [offset, lat, lon, alt, gs, track, 0, vrate, details, source,
            None, None, None, None]


@pytest.fixture
def traces(spark, tmp_path_factory):
    """Write one aircraft's trace to disk and read it back the way Bronze did."""
    def _make(trace, icao="a1b2c3"):
        root = tmp_path_factory.mktemp("t") / "v2025.12.30-planes-readsb-prod-0"
        directory = root / "traces" / "1c"
        directory.mkdir(parents=True)
        (directory / f"trace_full_{icao}.json.gz").write_bytes(
            gzip.compress(json.dumps({
                "icao": icao, "r": "HB-ABC", "t": "A320", "ownOp": "SWISS",
                "timestamp": DAY_START, "trace": trace,
            }).encode())
        )
        return decode(read_aircraft(spark, str(root)))

    return _make


def obs(icao="a1b2c3", offset_s=0, lat=47.0, lon=8.0, callsign="SWR1",
        on_ground=False, alt=10000.0, gs=300.0):
    """One decoded observation, ``offset_s`` after START."""
    return (icao, True, "HB-ABC", "A320", "SWISS", START + timedelta(seconds=offset_s),
            lat, lon, on_ground, alt, gs, 90.0, 0.0, callsign, "v2025.12.30",
            RELEASE_DATE)


# --- decoding ----------------------------------------------------------------


def test_decode_types_every_position_of_the_trace_point(spark, raw_path):
    rows = {
        (r.icao, r.event_time.isoformat()): r
        for r in decode(read_aircraft(spark, raw_path)).collect()
    }

    ground = rows[("7c6b1c", "2025-12-30T00:00:10")]
    assert ground.on_ground is True
    assert ground.altitude_ft is None, '"ground" is not an altitude'
    assert ground.callsign == "JST859", "callsign comes from the nested object"
    assert ground.aircraft_type == "A320"
    assert ground.registration == "VH-VFY"

    airborne = rows[("7c6b1c", "2025-12-30T01:00:00.500000")]
    assert airborne.on_ground is False
    assert airborne.altitude_ft == 27975.0
    assert airborne.ground_speed_kt == 434.1
    assert airborne.vertical_rate_fpm == 96.0
    assert airborne.callsign is None, "no nested object on this point"


def test_decode_keeps_sub_second_precision(traces):
    """Two receptions 0.48 s apart are two observations, not one.

    Second-grain timestamps would collapse 2.4M legitimate positions across a
    release; the dedup is not allowed to be papering over that.
    """
    rows = traces([point(40155.19), point(40155.67)]).collect()

    assert len(rows) == 2
    assert {r.event_time.microsecond for r in rows} == {190000, 670000}


def test_decode_keeps_observations_that_have_no_position(spark, raw_path):
    """Faithful to the source: bad rows are asserted against, never dropped."""
    assert decode(read_aircraft(spark, raw_path)).count() == 4


def test_decode_stamps_the_release_from_the_file_path(spark, raw_path):
    rows = decode(read_aircraft(spark, raw_path)).collect()

    assert {r.release_tag for r in rows} == {"v2025.12.30-planes-readsb-prod-0"}
    assert {r.release_date for r in rows} == {RELEASE_DATE}


# --- the dedup ---------------------------------------------------------------


def test_same_timestamp_receptions_collapse_to_the_most_complete_row(traces):
    """The real pattern: one point carries the details object, its twin does not."""
    rows = traces([
        point(100.0, lat=47.400, gs=None, track=None, vrate=None),
        point(100.0, lat=47.401, details={"flight": "SWR123 "}),
    ]).collect()

    assert len(rows) == 1
    assert rows[0].latitude == 47.401
    assert rows[0].callsign == "SWR123"


def test_the_dedup_ties_break_on_position_not_on_source_order(traces):
    """71% of real collisions tie on completeness. Pin what happens then."""
    first = traces([point(100.0, lat=47.402), point(100.0, lat=47.401)]).collect()
    second = traces([point(100.0, lat=47.401), point(100.0, lat=47.402)]).collect()

    assert len(first) == len(second) == 1
    assert first[0].latitude == second[0].latitude == 47.401


def test_the_dedup_ranks_on_raw_values_before_they_are_cleaned(traces):
    """An implausible speed is still evidence of a more complete reception.

    The ranking runs on the decoded values and the nulling runs after it, which
    is the order the previous Silver window used. Reversing them would change
    which row wins.
    """
    rows = traces([
        point(100.0, lat=47.400, gs=1800.0, track=90.0),
        point(100.0, lat=47.401, gs=None, track=None),
    ]).collect()

    assert len(rows) == 1
    assert rows[0].latitude == 47.400, "the fuller row won"
    assert rows[0].ground_speed_kt is None, "and its bad speed was then nulled"


def test_more_than_two_receptions_collapse_to_one(traces):
    """Up to eight share a timestamp in the real data."""
    rows = traces([
        point(100.0, lat=47.403), point(100.0, lat=47.401),
        point(100.0, lat=47.402), point(200.0, lat=47.500),
    ]).collect()

    assert sorted(r.latitude for r in rows) == [47.401, 47.500]


def test_distinct_timestamps_are_all_kept(traces):
    assert traces([point(100.0), point(105.0), point(110.0)]).count() == 3


def test_an_aircraft_with_no_collisions_is_untouched(traces):
    """0.16% of rows collide; the other 99.84% must pass through unchanged."""
    rows = traces([point(100.0 + i) for i in range(50)]).collect()

    assert len(rows) == 50
    assert len({r.event_time for r in rows}) == 50


# --- cleaning ----------------------------------------------------------------


def test_non_icao_addresses_are_flagged_not_dropped(traces):
    real = traces([point(100.0)], icao="a1b2c3").first()
    relayed = traces([point(100.0)], icao="~ab12cd").first()

    assert real.is_icao_address is True
    assert relayed.is_icao_address is False


@pytest.mark.parametrize("flight", ["", "   ", "        "])
def test_blank_callsigns_become_null(traces, flight):
    rows = traces([point(100.0, details={"flight": flight})]).collect()

    assert rows[0].callsign is None


def test_implausible_speed_is_nulled_but_the_position_is_kept(traces):
    """A bad speed must not cost us the position fix on that row."""
    row = traces([point(100.0, gs=1800.0)]).first()

    assert row.ground_speed_kt is None
    assert row.latitude == 47.4
    assert row.altitude_ft == 10000.0


def test_plausible_speed_survives(traces):
    """657 kt is a real B788 report in the sample; it must not be scrubbed."""
    assert traces([point(100.0, gs=657.0)]).first().ground_speed_kt == 657.0


def test_implausible_vertical_rate_is_nulled(traces):
    assert traces([point(100.0, vrate=-64000)]).first().vertical_rate_fpm is None


# --- segmentation ------------------------------------------------------------


def test_observations_within_the_gap_stay_one_flight(cleaned):
    df = assign_flights(cleaned([obs(offset_s=0), obs(offset_s=60), obs(offset_s=120)]))

    assert df.select("flight_id").distinct().count() == 1


def test_a_gap_longer_than_the_threshold_starts_a_new_flight(cleaned):
    df = assign_flights(cleaned([obs(offset_s=0), obs(offset_s=GAP_SECONDS + 1)]))

    assert df.select("flight_id").distinct().count() == 2


def test_a_gap_exactly_at_the_threshold_does_not_split(cleaned):
    """The rule is strictly greater-than; pin the boundary."""
    df = assign_flights(cleaned([obs(offset_s=0), obs(offset_s=GAP_SECONDS)]))

    assert df.select("flight_id").distinct().count() == 1


def test_aircraft_are_segmented_independently(cleaned):
    df = assign_flights(cleaned([
        obs(icao="aaa111", offset_s=0), obs(icao="bbb222", offset_s=0),
        obs(icao="aaa111", offset_s=60), obs(icao="bbb222", offset_s=60),
    ]))

    assert df.select("flight_id").distinct().count() == 2


def test_flight_id_is_the_address_and_the_first_instant(cleaned):
    row = assign_flights(cleaned([obs(offset_s=0), obs(offset_s=60)])).first()

    assert row.flight_id == "a1b2c3_20251230080000"


def test_observation_seq_is_a_gap_free_sequence_per_flight(cleaned):
    rows = assign_flights(
        cleaned([obs(offset_s=s) for s in (0, 30, 60, 90)])
    ).collect()

    assert sorted(r.observation_seq for r in rows) == [1, 2, 3, 4]


def test_segmentation_is_reproducible_whatever_order_the_rows_arrive_in(cleaned):
    rows = [obs(offset_s=s) for s in (0, 60, GAP_SECONDS + 61, GAP_SECONDS + 121)]

    forward = assign_flights(cleaned(rows)).collect()
    backward = assign_flights(cleaned(list(reversed(rows)))).collect()

    key = lambda r: (r.flight_id, r.observation_seq)  # noqa: E731
    assert sorted(map(key, forward)) == sorted(map(key, backward))


# --- the whole pass ----------------------------------------------------------


def test_to_observations_produces_the_published_columns(spark, raw_path):
    df = to_observations(read_aircraft(spark, raw_path))

    assert df.columns == [
        "flight_id", "icao", "is_icao_address", "registration", "aircraft_type",
        "operator", "event_time", "observation_seq", "latitude", "longitude",
        "on_ground", "altitude_ft", "ground_speed_kt", "track_deg",
        "vertical_rate_fpm", "callsign", "release_tag", "release_date",
    ]
    assert "source_file" not in df.columns, "an 88-char URL on every row"


def test_observations_round_trip_through_delta(spark, raw_path, tmp_path):
    path = str(tmp_path / "observations")
    observations = to_observations(read_aircraft(spark, raw_path))

    write_observations(observations, path, release_date="2025-12-30")
    table = read_observations(spark, path)

    assert table.count() == observations.count()
    assert (tmp_path / "observations" / "_delta_log").is_dir(), "no transaction log"
    # the "ground" sentinel survived the round trip as a typed pair of columns
    ground = table.where("on_ground").collect()
    assert len(ground) == 1
    assert ground[0].altitude_ft is None

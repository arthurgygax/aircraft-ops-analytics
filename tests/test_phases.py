from datetime import date, datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark", reason="Spark tests run in the spark container")

from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from adsb.phases import label_observations, to_flight_phases  # noqa: E402

# the flight_observations columns the phase logic reads
POINT_SCHEMA = StructType([
    StructField("flight_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("observation_seq", IntegerType()),
    StructField("altitude_ft", DoubleType()),
    StructField("on_ground", BooleanType()),
    StructField("ground_speed_kt", DoubleType()),
    StructField("vertical_rate_fpm", DoubleType()),
    StructField("release_date", DateType()),
])

START = datetime(2025, 12, 30, 8, 0, 0)
RELEASE_DATE = date(2025, 12, 30)
STEP = 10  # seconds between observations, close to the real median


def track(points, flight_id="a1b2c3_20251230080000"):
    """Build a trajectory from (altitude, vertical_rate, on_ground) triples."""
    rows = []
    for i, (alt, vrate, on_ground) in enumerate(points):
        rows.append((
            flight_id,
            START + timedelta(seconds=i * STEP),
            i + 1,
            alt,
            on_ground,
            15.0 if on_ground else 300.0,
            vrate,
            RELEASE_DATE,
        ))
    return rows


def ground(n):
    return [(None, None, True)] * n


def climbing(n, rate=1800.0, base=1000.0):
    return [(base + i * 300.0, rate, False) for i in range(n)]


def level(n, alt=35000.0):
    return [(alt, 0.0, False) for i in range(n)]


def descending(n, rate=-1500.0, base=35000.0):
    return [(base - i * 250.0, rate, False) for i in range(n)]


@pytest.fixture
def points(spark):
    return lambda rows: spark.createDataFrame(rows, POINT_SCHEMA)


def phases_of(df):
    return [
        (r.phase, r.n_observations)
        for r in sorted(to_flight_phases(df).collect(), key=lambda r: r.phase_seq)
    ]


def labels_of(df):
    return [
        r.phase
        for r in sorted(label_observations(df).collect(), key=lambda r: r.observation_seq)
    ]


# --- the core phases ---------------------------------------------------------


def test_a_sustained_climb_is_detected(points):
    assert labels_of(points(track(climbing(20)))) == ["climb"] * 20


def test_level_flight_is_cruise(points):
    assert labels_of(points(track(level(20)))) == ["cruise"] * 20


def test_a_sustained_descent_is_detected(points):
    assert labels_of(points(track(descending(20)))) == ["descent"] * 20


def test_a_full_profile_collapses_into_ordered_phases(points):
    """Ground, climb, cruise, descent, ground -- the shape of a real flight."""
    profile = (
        ground(10)
        + climbing(30)
        + level(30)
        + descending(30)
        + ground(10)
    )

    assert [p for p, _ in phases_of(points(track(profile)))] == [
        "taxi_out", "climb", "cruise", "descent", "taxi_in",
    ]


def test_ground_time_is_taxi_out_before_and_taxi_in_after_the_air(points):
    profile = ground(5) + climbing(20) + descending(20) + ground(5)

    result = dict(phases_of(points(track(profile))))

    assert result["taxi_out"] == 5
    assert result["taxi_in"] == 5


def test_ground_between_two_airborne_runs_is_plain_taxi(points):
    """A turnaround inside one reconstructed flight: direction is unknowable."""
    profile = climbing(15) + descending(15) + ground(10) + climbing(15)

    assert "taxi" in [p for p, _ in phases_of(points(track(profile)))]


def test_a_flight_that_never_leaves_the_ground_is_taxi(points):
    assert labels_of(points(track(ground(10)))) == ["taxi"] * 10


# --- intervals ---------------------------------------------------------------


def test_intervals_carry_times_duration_and_counts(points):
    profile = climbing(10) + level(10)

    runs = sorted(to_flight_phases(points(track(profile))).collect(),
                  key=lambda r: r.phase_seq)

    assert [r.phase for r in runs] == ["climb", "cruise"]
    assert runs[0].start_time == START
    assert runs[0].n_observations + runs[1].n_observations == 20
    for r in runs:
        expected = (r.end_time - r.start_time).total_seconds()
        assert r.duration_seconds == expected


def test_phase_seq_orders_the_phases_within_a_flight(points):
    profile = ground(5) + climbing(20) + level(20)

    runs = to_flight_phases(points(track(profile))).collect()
    ordered = sorted(runs, key=lambda r: r.phase_seq)

    assert [r.phase_seq for r in ordered] == [1, 2, 3]
    assert [r.start_time for r in ordered] == sorted(r.start_time for r in runs)


def test_flights_are_phased_independently(points):
    rows = track(climbing(15), flight_id="aaa111_20251230080000") + track(
        descending(15), flight_id="bbb222_20251230080000"
    )

    by_flight = {}
    for r in to_flight_phases(points(rows)).collect():
        by_flight.setdefault(r.flight_id, []).append(r.phase)

    assert by_flight["aaa111_20251230080000"] == ["climb"]
    assert by_flight["bbb222_20251230080000"] == ["descent"]


# --- missing and noisy data --------------------------------------------------


def test_airborne_points_with_no_vertical_rate_are_unknown_not_guessed(points):
    profile = [(20000.0, None, False)] * 15

    assert labels_of(points(track(profile))) == ["unknown"] * 15


def test_an_unknown_ground_state_is_not_forced_into_a_phase(points):
    profile = [(None, None, None)] * 10

    assert labels_of(points(track(profile))) == ["unknown"] * 10


def test_a_gap_in_vertical_rate_is_bridged_by_the_smoothing_window(points):
    """One missing sample inside a climb must not manufacture a phase change."""
    profile = climbing(10)
    profile[5] = (profile[5][0], None, False)

    assert labels_of(points(track(profile))) == ["climb"] * 10


def test_noise_does_not_fragment_a_steady_climb(points):
    """Alternating spikes around a real climb: smoothing must absorb them."""
    profile = [
        (1000.0 + i * 300.0, 1800.0 + (900.0 if i % 2 else -900.0), False)
        for i in range(20)
    ]

    assert phases_of(points(track(profile))) == [("climb", 20)]


def test_a_single_noisy_spike_in_cruise_does_not_create_a_climb(points):
    profile = level(20)
    profile[10] = (35000.0, 2000.0, False)  # one bad sample

    assert phases_of(points(track(profile))) == [("cruise", 20)]


def test_shallow_drift_inside_the_level_band_stays_cruise(points):
    """+/-200 fpm is inside the 300 fpm band and must not read as a climb."""
    profile = [(35000.0, 200.0, False) for _ in range(15)]

    assert labels_of(points(track(profile))) == ["cruise"] * 15


def test_the_level_band_is_adjustable(points):
    """A 100 fpm band makes the same 200 fpm drift a climb."""
    df = points(track([(35000.0, 200.0, False) for _ in range(15)]))

    labels = {
        r.phase for r in label_observations(df, level_band_fpm=100.0).collect()
    }

    assert labels == {"climb"}


# --- determinism -------------------------------------------------------------


def test_phase_output_is_deterministic(points):
    profile = ground(5) + climbing(20) + level(20) + descending(20) + ground(5)

    def snapshot():
        return [
            (r.phase_seq, r.phase, r.n_observations)
            for r in sorted(to_flight_phases(points(track(profile))).collect(),
                            key=lambda r: r.phase_seq)
        ]

    assert snapshot() == snapshot()

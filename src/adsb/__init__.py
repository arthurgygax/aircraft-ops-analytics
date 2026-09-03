"""ADS-B data engineering pipeline (adsb.lol globe_history source).

This package is the new pipeline and is independent of the legacy
OpenSky/Streamlit application (``convert_to_parquet.py``, ``logic.py``,
``app.py``, ``viz.py``), which lives beside it in ``src/`` and reads a
different source format.

Nothing is implemented yet: ingestion is added in the next phase.
"""

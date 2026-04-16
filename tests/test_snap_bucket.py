"""Tests for the canonical-bucket snapping used to collapse offset-shifted bars."""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.datalake import snap_to_canonical_bucket


def _ts(*values):
    return pd.Series(pd.to_datetime(list(values), utc=True))


class TestSnapToCanonicalBucket:
    def test_d1_collapses_hour_offsets_to_utc_midnight(self):
        series = _ts("2018-01-02 00:00:00", "2018-01-02 01:00:00", "2018-01-02 02:00:00")
        out = snap_to_canonical_bucket(series, "D1")
        assert (out == _ts("2018-01-02", "2018-01-02", "2018-01-02")).all()

    def test_h4_snaps_to_nearest_4h_anchor(self):
        series = _ts(
            "2018-01-02 00:00:00", "2018-01-02 01:00:00",  # → 00
            "2018-01-02 04:00:00", "2018-01-02 05:00:00",  # → 04
            "2018-01-02 21:00:00", "2018-01-02 22:00:00",  # → 20
        )
        out = snap_to_canonical_bucket(series, "H4")
        expected = _ts(
            "2018-01-02 00:00:00", "2018-01-02 00:00:00",
            "2018-01-02 04:00:00", "2018-01-02 04:00:00",
            "2018-01-02 20:00:00", "2018-01-02 20:00:00",
        )
        assert (out == expected).all()

    def test_h1_is_noop_on_hour_aligned_stamps(self):
        series = _ts("2018-01-02 00:00:00", "2018-01-02 01:00:00", "2018-01-02 23:00:00")
        out = snap_to_canonical_bucket(series, "H1")
        assert (out == series).all()

    def test_m5_snaps_minute_to_5min(self):
        series = _ts("2018-01-02 00:03:00", "2018-01-02 00:07:30")
        out = snap_to_canonical_bucket(series, "M5")
        assert (out == _ts("2018-01-02 00:00:00", "2018-01-02 00:05:00")).all()

    def test_w1_anchors_to_monday(self):
        # Wednesday 2018-01-03 → Monday 2018-01-01
        series = _ts("2018-01-03 10:00:00", "2018-01-07 23:59:00")  # Wed, Sun
        out = snap_to_canonical_bucket(series, "W1")
        assert (out == _ts("2018-01-01", "2018-01-01")).all()

    def test_mn1_anchors_to_month_start(self):
        series = _ts("2018-01-15 10:00:00", "2018-03-31 23:59:00")
        out = snap_to_canonical_bucket(series, "MN1")
        assert (out == _ts("2018-01-01", "2018-03-01")).all()

    def test_unknown_timeframe_passthrough(self):
        series = _ts("2018-01-02 00:30:00")
        out = snap_to_canonical_bucket(series, "WEIRD9")
        assert (out == series).all()

    def test_case_insensitive(self):
        series = _ts("2018-01-02 01:00:00")
        out = snap_to_canonical_bucket(series, "d1")
        assert (out == _ts("2018-01-02")).all()

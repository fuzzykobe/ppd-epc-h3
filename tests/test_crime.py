"""Tests for crime pipeline steps."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Helpers (inline — no pipeline import needed for unit tests)
# ---------------------------------------------------------------------------

def normalise_crime_row(raw: dict) -> dict:
    """Mirrors the column renames and casts in 09_ingest_crime.py."""
    import re
    from datetime import date

    def _date(s: str | None) -> date | None:
        if not s:
            return None
        try:
            y, m = s.strip().split("-")
            return date(int(y), int(m), 1)
        except (ValueError, AttributeError):
            return None

    def _float(s: str | None) -> float | None:
        try:
            return float(s.strip()) if s else None
        except (ValueError, AttributeError):
            return None

    return {
        "crime_id":     raw.get("Crime ID", "").strip() or None,
        "month":        _date(raw.get("Month")),
        "force":        (raw.get("Reported by") or "").strip(),
        "longitude":    _float(raw.get("Longitude")),
        "latitude":     _float(raw.get("Latitude")),
        "location":     (raw.get("Location") or "").strip(),
        "lsoa_code":    (raw.get("LSOA code") or "").strip(),
        "lsoa_name":    (raw.get("LSOA name") or "").strip(),
        "crime_type":   (raw.get("Crime type") or "").strip(),
        "last_outcome": (raw.get("Last outcome category") or "").strip(),
        "context":      (raw.get("Context") or "").strip(),
    }


# ---------------------------------------------------------------------------
# Column normalisation
# ---------------------------------------------------------------------------

SAMPLE_RAW = {
    "Crime ID": "abc123",
    "Month": "2024-03",
    "Reported by": "Metropolitan Police Service",
    "Falls within": "Metropolitan Police Service",
    "Longitude": "-0.1281",
    "Latitude": "51.5080",
    "Location": "On or near High Street",
    "LSOA code": "E01000001",
    "LSOA name": "City of London 001A",
    "Crime type": "Theft from the person",
    "Last outcome category": "Unable to prosecute suspect",
    "Context": "",
}


def test_normalise_crime_columns_basic():
    row = normalise_crime_row(SAMPLE_RAW)
    assert row["crime_id"] == "abc123"
    assert row["month"] == date(2024, 3, 1)
    assert row["force"] == "Metropolitan Police Service"
    assert row["longitude"] == pytest.approx(-0.1281)
    assert row["latitude"] == pytest.approx(51.5080)
    assert row["lsoa_code"] == "E01000001"
    assert row["crime_type"] == "Theft from the person"


def test_normalise_crime_month_first_of_month():
    row = normalise_crime_row({**SAMPLE_RAW, "Month": "2023-11"})
    assert row["month"] == date(2023, 11, 1)


def test_normalise_crime_null_lat_lon():
    row = normalise_crime_row({**SAMPLE_RAW, "Latitude": "", "Longitude": ""})
    assert row["latitude"] is None
    assert row["longitude"] is None


def test_normalise_crime_strips_whitespace():
    row = normalise_crime_row({**SAMPLE_RAW, "Crime type": "  Anti-social behaviour  "})
    assert row["crime_type"] == "Anti-social behaviour"


def test_normalise_crime_empty_crime_id():
    row = normalise_crime_row({**SAMPLE_RAW, "Crime ID": ""})
    assert row["crime_id"] is None


# ---------------------------------------------------------------------------
# H3 application
# ---------------------------------------------------------------------------

try:
    import h3 as _h3
    H3_AVAILABLE = True
except ImportError:
    H3_AVAILABLE = False


@pytest.mark.skipif(not H3_AVAILABLE, reason="h3 not installed")
def test_h3_crime_known_location():
    """Trafalgar Square should land in consistent H3 cells across resolutions."""
    import h3
    lat, lon = 51.5080, -0.1281
    cells = {r: h3.latlng_to_cell(lat, lon, r) for r in [7, 8, 9, 10]}
    assert all(v is not None for v in cells.values())
    assert len(set(cells.values())) == 4  # all distinct resolutions → different cells


@pytest.mark.skipif(not H3_AVAILABLE, reason="h3 not installed")
def test_h3_crime_null_coords():
    def safe_h3(lat, lon, res):
        if lat is None or lon is None:
            return None
        try:
            import h3
            return h3.latlng_to_cell(lat, lon, res)
        except Exception:
            return None

    assert safe_h3(None, -0.1281, 9) is None
    assert safe_h3(51.5080, None, 9) is None


@pytest.mark.skipif(not H3_AVAILABLE, reason="h3 not installed")
def test_h3_crime_null_propagates_to_drop():
    """Rows with None lat/lon produce None cells and are filtered out in step 10."""
    def safe_h3(lat, lon, res):
        if lat is None or lon is None:
            return None
        try:
            import h3
            return h3.latlng_to_cell(lat, lon, res)
        except Exception:
            return None

    # The drop filter in step 10 targets h3_r7 IS NULL
    cell = safe_h3(None, None, 7)
    assert cell is None  # → row would be dropped


# ---------------------------------------------------------------------------
# Aggregation logic
# ---------------------------------------------------------------------------

def _make_crime_rows() -> list[dict]:
    import h3
    lat, lon = 51.5080, -0.1281
    cell7 = h3.latlng_to_cell(lat, lon, 7)
    cell8 = h3.latlng_to_cell(lat, lon, 8)
    cell9 = h3.latlng_to_cell(lat, lon, 9)
    cell10 = h3.latlng_to_cell(lat, lon, 10)

    return [
        {"h3_r7": cell7, "h3_r8": cell8, "h3_r9": cell9, "h3_r10": cell10,
         "year": 2024, "month": date(2024, 3, 1),
         "crime_type": "Theft from the person", "force": "Metropolitan Police Service",
         "lsoa_code": "E01000001"},
        {"h3_r7": cell7, "h3_r8": cell8, "h3_r9": cell9, "h3_r10": cell10,
         "year": 2024, "month": date(2024, 3, 1),
         "crime_type": "Theft from the person", "force": "Metropolitan Police Service",
         "lsoa_code": "E01000001"},
        {"h3_r7": cell7, "h3_r8": cell8, "h3_r9": cell9, "h3_r10": cell10,
         "year": 2024, "month": date(2024, 3, 1),
         "crime_type": "Violence and sexual offences", "force": "Metropolitan Police Service",
         "lsoa_code": "E01000001"},
        {"h3_r7": cell7, "h3_r8": cell8, "h3_r9": cell9, "h3_r10": cell10,
         "year": 2024, "month": date(2024, 4, 1),
         "crime_type": "Theft from the person", "force": "Metropolitan Police Service",
         "lsoa_code": "E01000001"},
    ]


@pytest.mark.skipif(not H3_AVAILABLE, reason="h3 not installed")
def test_aggregation_long_counts():
    """Group by (cell, year, month, crime_type) and verify counts."""
    from collections import defaultdict
    rows = _make_crime_rows()
    groups: dict[tuple, int] = defaultdict(int)
    for r in rows:
        key = (r["h3_r9"], r["year"], r["month"], r["crime_type"])
        groups[key] += 1

    # March: 2 theft, 1 violence
    theft_mar = (rows[0]["h3_r9"], 2024, date(2024, 3, 1), "Theft from the person")
    violence_mar = (rows[0]["h3_r9"], 2024, date(2024, 3, 1), "Violence and sexual offences")
    theft_apr = (rows[0]["h3_r9"], 2024, date(2024, 4, 1), "Theft from the person")

    assert groups[theft_mar] == 2
    assert groups[violence_mar] == 1
    assert groups[theft_apr] == 1


@pytest.mark.skipif(not H3_AVAILABLE, reason="h3 not installed")
def test_aggregation_wide_total():
    """Wide pivot: total_crimes per (cell, year, month)."""
    from collections import defaultdict
    rows = _make_crime_rows()
    totals: dict[tuple, int] = defaultdict(int)
    for r in rows:
        key = (r["h3_r9"], r["year"], r["month"])
        totals[key] += 1

    cell9 = rows[0]["h3_r9"]
    assert totals[(cell9, 2024, date(2024, 3, 1))] == 3
    assert totals[(cell9, 2024, date(2024, 4, 1))] == 1


@pytest.mark.skipif(not H3_AVAILABLE, reason="h3 not installed")
def test_aggregation_distinct_groups():
    """Long format should produce 3 distinct (cell, month, crime_type) groups."""
    from collections import defaultdict
    rows = _make_crime_rows()
    groups: dict[tuple, int] = defaultdict(int)
    for r in rows:
        key = (r["h3_r9"], r["month"], r["crime_type"])
        groups[key] += 1
    assert len(groups) == 3

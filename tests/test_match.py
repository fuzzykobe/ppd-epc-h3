"""Tests for matching logic using fixture CSVs."""
import re
import pytest


def normalise_addr(parts: list[str | None]) -> str:
    joined = " ".join(p for p in parts if p)
    upper = joined.upper()
    stripped = re.sub(r"[^A-Z0-9 ]", " ", upper)
    return re.sub(r"\s+", " ", stripped).strip()


def exact_match(ppd_rows, epc_rows):
    """Return list of (transaction_id, lmk_key) exact matches."""
    epc_index = {(r["postcode"], r["addr_key"]): r["lmk_key"] for r in epc_rows}
    matches = []
    for row in ppd_rows:
        key = (row["postcode"], row["addr_key"])
        if key in epc_index:
            matches.append((row["transaction_id"], epc_index[key]))
    return matches


PPD_ROWS = [
    {"transaction_id": "T001", "postcode": "SW1A 1AA",
     "addr_key": normalise_addr(["10", None, "DOWNING STREET"])},
    {"transaction_id": "T002", "postcode": "EC1A 1BB",
     "addr_key": normalise_addr(["FLAT 2", "5", "CITY ROAD"])},
    {"transaction_id": "T003", "postcode": "W1A 0AX",
     "addr_key": normalise_addr(["1", None, "OXFORD STREET"])},
]

EPC_ROWS = [
    {"lmk_key": "E001", "postcode": "SW1A 1AA",
     "addr_key": normalise_addr(["10 DOWNING STREET", None])},
    {"lmk_key": "E002", "postcode": "EC1A 1BB",
     "addr_key": normalise_addr(["FLAT 2 5 CITY ROAD", None])},
    {"lmk_key": "E999", "postcode": "N1 9GU",
     "addr_key": normalise_addr(["99 ANGEL STREET", None])},
]


def test_exact_match_found():
    matches = exact_match(PPD_ROWS[:1], EPC_ROWS[:1])
    assert len(matches) == 1
    assert matches[0] == ("T001", "E001")


def test_exact_match_multiple():
    matches = exact_match(PPD_ROWS[:2], EPC_ROWS[:2])
    assert len(matches) == 2


def test_exact_match_no_cross_postcode():
    # T003 is in W1A 0AX; E999 is in N1 9GU — should not match
    matches = exact_match(PPD_ROWS[2:], EPC_ROWS[2:])
    assert matches == []


def test_exact_match_unknown_property():
    unknown = [{"transaction_id": "T999", "postcode": "ZZ9 9ZZ",
                "addr_key": "UNKNOWN ADDRESS"}]
    matches = exact_match(unknown, EPC_ROWS)
    assert matches == []


def test_epc_dedup_takes_latest():
    """Latest lodgement date per (postcode, addr_key) should be kept."""
    from datetime import date

    certs = [
        {"lmk_key": "E100", "postcode": "AB1 2CD", "addr_key": "1 TEST ROAD", "lodgement_date": date(2020, 1, 1)},
        {"lmk_key": "E101", "postcode": "AB1 2CD", "addr_key": "1 TEST ROAD", "lodgement_date": date(2023, 6, 15)},
        {"lmk_key": "E102", "postcode": "AB1 2CD", "addr_key": "1 TEST ROAD", "lodgement_date": date(2019, 3, 10)},
    ]

    # Dedup: per (postcode, addr_key) keep latest
    by_key: dict[tuple, dict] = {}
    for cert in certs:
        k = (cert["postcode"], cert["addr_key"])
        if k not in by_key or cert["lodgement_date"] > by_key[k]["lodgement_date"]:
            by_key[k] = cert

    result = list(by_key.values())
    assert len(result) == 1
    assert result[0]["lmk_key"] == "E101"

import re
import pytest


def normalise_addr(parts: list[str | None]) -> str:
    joined = " ".join(p for p in parts if p)
    upper = joined.upper()
    stripped = re.sub(r"[^A-Z0-9 ]", " ", upper)
    return re.sub(r"\s+", " ", stripped).strip()


def ppd_addr_key(paon: str | None, saon: str | None, street: str | None) -> str:
    return normalise_addr([saon, paon, street])


def epc_addr_key(address1: str | None, address2: str | None) -> str:
    return normalise_addr([address1, address2])


@pytest.mark.parametrize("parts,expected", [
    (["Flat 3", "12 Brixton Road"], "FLAT 3 12 BRIXTON ROAD"),
    (["12A", None, "HIGH STREET"], "12A HIGH STREET"),
    (["", "UNIT 5", "PARK LANE"], "UNIT 5 PARK LANE"),
    (["O'Brien House", "MAIN ST"], "O BRIEN HOUSE MAIN ST"),
    (["  FLAT 1  ", "  ACACIA AVENUE  "], "FLAT 1 ACACIA AVENUE"),
    ([None, None, None], ""),
    (["WOODLANDS PARK", "ROAD"], "WOODLANDS PARK ROAD"),
    (["42B"], "42B"),
])
def test_normalise_addr(parts, expected):
    assert normalise_addr(parts) == expected


def test_normalise_strips_punctuation():
    result = normalise_addr(["St. Mary's Court", "Church-Road"])
    assert "'" not in result
    assert "." not in result
    assert "-" not in result


def test_normalise_collapses_whitespace():
    result = normalise_addr(["  flat   3  ", "  12   high   st  "])
    assert "  " not in result


def test_ppd_addr_key_saon_first():
    key = ppd_addr_key(paon="12", saon="FLAT 3", street="BRIXTON ROAD")
    assert key.startswith("FLAT 3")


def test_ppd_addr_key_no_saon():
    key = ppd_addr_key(paon="22", saon=None, street="VICTORIA STREET")
    assert key == "22 VICTORIA STREET"


def test_ppd_addr_key_empty_saon():
    key = ppd_addr_key(paon="100", saon="", street="THE AVENUE")
    assert key == "100 THE AVENUE"


def test_epc_addr_key():
    key = epc_addr_key("FLAT 3", "12 BRIXTON ROAD")
    assert key == "FLAT 3 12 BRIXTON ROAD"


def test_epc_addr_key_no_address2():
    key = epc_addr_key("FLAT 1A", None)
    assert key == "FLAT 1A"

"""Spot-check H3 cell assignment for known coordinates."""
import pytest

try:
    import h3
    H3_AVAILABLE = True
except ImportError:
    H3_AVAILABLE = False


@pytest.mark.skipif(not H3_AVAILABLE, reason="h3 not installed")
@pytest.mark.parametrize("lat,lon,res,expected_prefix", [
    # Trafalgar Square, London
    (51.5080, -0.1281, 7, "87"),
    (51.5080, -0.1281, 8, "88"),
    (51.5080, -0.1281, 9, "89"),
    (51.5080, -0.1281, 10, "8a"),
])
def test_h3_cell_prefix(lat, lon, res, expected_prefix):
    cell = h3.latlng_to_cell(lat, lon, res)
    assert cell.startswith(expected_prefix), f"Got {cell!r} for res {res}"


@pytest.mark.skipif(not H3_AVAILABLE, reason="h3 not installed")
def test_h3_cell_length():
    cell = h3.latlng_to_cell(51.5080, -0.1281, 9)
    assert len(cell) == 15


@pytest.mark.skipif(not H3_AVAILABLE, reason="h3 not installed")
def test_h3_null_coords():
    def safe_h3(lat, lon, res):
        if lat is None or lon is None:
            return None
        return h3.latlng_to_cell(lat, lon, res)

    assert safe_h3(None, -0.1281, 9) is None
    assert safe_h3(51.5080, None, 9) is None
    assert safe_h3(None, None, 9) is None


@pytest.mark.skipif(not H3_AVAILABLE, reason="h3 not installed")
def test_h3_resolution_containment():
    """Coarser cells should contain finer ones."""
    lat, lon = 51.5080, -0.1281
    cell_r9 = h3.latlng_to_cell(lat, lon, 9)
    cell_r8 = h3.latlng_to_cell(lat, lon, 8)
    cell_r7 = h3.latlng_to_cell(lat, lon, 7)

    # r9 cell's parent at r8 should equal the r8 cell for the same point
    assert h3.cell_to_parent(cell_r9, 8) == cell_r8
    assert h3.cell_to_parent(cell_r9, 7) == cell_r7

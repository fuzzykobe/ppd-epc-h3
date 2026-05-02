"""Apply H3 spatial indexing at resolutions 7, 8, 9, 10."""
import click
import polars as pl
import h3
from loguru import logger

from pipeline.config import DATA_STAGED, H3_RESOLUTIONS


def _h3_cell(lat: float | None, lon: float | None, res: int) -> str | None:
    if lat is None or lon is None:
        return None
    try:
        return h3.latlng_to_cell(lat, lon, res)
    except Exception:
        return None


@click.command()
def main() -> None:
    geo_path = DATA_STAGED / "transactions_geo.parquet"
    out = DATA_STAGED / "transactions_h3.parquet"

    if not geo_path.exists():
        raise FileNotFoundError(f"{geo_path} not found")

    pl.Config.set_streaming_chunk_size(1_000_000)  # larger chunks for 48GB RAM

    logger.info(f"Reading {geo_path}")
    df = pl.read_parquet(str(geo_path))

    for res in H3_RESOLUTIONS:
        logger.info(f"Computing H3 resolution {res}")
        df = df.with_columns(
            pl.struct(["lat", "lon"])
            .map_elements(
                lambda s, r=res: _h3_cell(s["lat"], s["lon"], r),
                return_dtype=pl.Utf8,
            )
            .alias(f"h3_r{res}")
        )

    df.write_parquet(str(out), compression="zstd", compression_level=6)

    h3_count = df.filter(pl.col("h3_r7").is_not_null()).height
    logger.success(
        f"transactions_h3.parquet written — {df.height:,} rows, "
        f"{100.0 * h3_count / df.height:.1f}% have H3 index"
    )


if __name__ == "__main__":
    main()

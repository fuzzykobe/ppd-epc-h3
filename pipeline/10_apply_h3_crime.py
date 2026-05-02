"""Apply H3 spatial indexing to crime records at resolutions 7, 8, 9, 10."""
import click
import h3
import polars as pl
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
    src = DATA_STAGED / "stg_crime.parquet"
    out = DATA_STAGED / "crime_h3.parquet"

    if not src.exists():
        raise FileNotFoundError(f"{src} not found — run step 09 first")

    pl.Config.set_streaming_chunk_size(1_000_000)

    logger.info(f"Reading {src}")
    df = pl.read_parquet(str(src))
    before = df.height
    logger.info(f"Rows before H3: {before:,}")

    for res in H3_RESOLUTIONS:
        logger.info(f"Computing H3 resolution {res}")
        df = df.with_columns(
            pl.struct(["latitude", "longitude"])
            .map_elements(
                lambda s, r=res: _h3_cell(s["latitude"], s["longitude"], r),
                return_dtype=pl.Utf8,
            )
            .alias(f"h3_r{res}")
        )

    # Drop rows where any H3 cell is null (bad / out-of-range coordinates)
    df = df.filter(pl.col("h3_r7").is_not_null())
    dropped = before - df.height
    if dropped:
        logger.warning(f"Dropped {dropped:,} rows with null H3 cells (bad coordinates)")

    df.write_parquet(str(out), compression="zstd", compression_level=6)
    logger.success(
        f"crime_h3.parquet written — {df.height:,} rows "
        f"({100.0 * df.height / before:.2f}% retained)"
    )


if __name__ == "__main__":
    main()

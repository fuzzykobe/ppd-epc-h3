"""Orchestrator — runs all pipeline steps in order."""
import platform
import time
import click
from loguru import logger


def _check_arm64() -> None:
    arch = platform.machine()
    if arch != "arm64":
        logger.warning(
            f"Expected native arm64 binary, got '{arch}'. "
            "Run: python -c \"import platform; print(platform.machine())\""
        )

STEPS = [
    (1,  "01_ingest_ppd",     "Ingest PPD"),
    (2,  "02_ingest_epc",     "Ingest EPC"),
    (3,  "03_ingest_ons",     "Ingest ONS"),
    (4,  "04_normalise",      "Validate normalisation"),
    (5,  "05_match",          "Address matching"),
    (6,  "06_enrich_geo",     "Enrich geography"),
    (7,  "07_apply_h3",       "Apply H3 indexes"),
    (8,  "08_build_mart",     "Build mart"),
    (9,  "09_ingest_crime",   "Ingest crime data"),
    (10, "10_apply_h3_crime", "Apply H3 to crime"),
    (11, "11_aggregate_crime",       "Aggregate crime mart"),
    (12, "12_ingest_income",         "Ingest ONS household income"),
    (13, "13_build_price_income_mart","Build price-to-income mart"),
]


@click.command()
@click.option("--from-step", default=1, type=int, show_default=True,
              help="Resume from this step number (1–13)")
def main(from_step: int) -> None:
    _check_arm64()
    total_start = time.perf_counter()

    for step_num, module_name, description in STEPS:
        if step_num < from_step:
            logger.info(f"Skipping step {step_num}: {description}")
            continue

        logger.info(f"{'='*60}")
        logger.info(f"Step {step_num}: {description}")
        logger.info(f"{'='*60}")

        import importlib
        t0 = time.perf_counter()
        try:
            mod = importlib.import_module(f"pipeline.{module_name}")
            mod.main.main(standalone_mode=False)
        except Exception as exc:
            logger.error(f"Step {step_num} FAILED: {exc}")
            raise SystemExit(1) from exc

        elapsed = time.perf_counter() - t0
        logger.success(f"Step {step_num} complete in {elapsed:.1f}s")

    total = time.perf_counter() - total_start
    logger.success(f"Pipeline complete in {total:.1f}s")


if __name__ == "__main__":
    main()

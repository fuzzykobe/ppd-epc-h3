"""Ingest EPC certificates CSVs → data/staged/stg_epc.parquet (deduplicated)"""
import click
import duckdb
from loguru import logger

from pipeline.config import DATA_STAGED, DUCKDB_MEMORY, DUCKDB_THREADS, EPC_DIR, EPC_KEEP_COLUMNS


@click.command()
def main() -> None:
    glob = str(EPC_DIR / "**" / "certificates.csv")
    import glob as glob_module
    matches = glob_module.glob(glob, recursive=True)
    if not matches:
        raise FileNotFoundError(f"No certificates.csv files found under {EPC_DIR}")

    DATA_STAGED.mkdir(parents=True, exist_ok=True)
    out = DATA_STAGED / "stg_epc.parquet"

    logger.info(f"Found {len(matches)} certificates.csv files")

    keep_cols_sql = ", ".join(
        f"upper(trim(CAST({c} AS VARCHAR))) AS {c.lower()}"
        if c not in ("LMK_KEY", "UPRN", "REPORT_TYPE", "LODGEMENT_DATE",
                     "LODGEMENT_DATETIME", "INSPECTION_DATE",
                     "TOTAL_FLOOR_AREA", "NUMBER_HABITABLE_ROOMS", "NUMBER_HEATED_ROOMS",
                     "FLAT_STOREY_COUNT", "EXTENSION_COUNT",
                     "CURRENT_ENERGY_EFFICIENCY", "POTENTIAL_ENERGY_EFFICIENCY",
                     "ENVIRONMENT_IMPACT_CURRENT", "CO2_EMISSIONS_CURRENT",
                     "CO2_EMISS_CURR_PER_FLOOR_AREA", "ENERGY_CONSUMPTION_CURRENT",
                     "HEATING_COST_CURRENT", "LIGHTING_COST_CURRENT", "HOT_WATER_COST_CURRENT")
        else f"TRY_CAST({c} AS VARCHAR) AS {c.lower()}"
        for c in EPC_KEEP_COLUMNS
    )

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    con.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY}'")

    # Deduplicate: per (postcode, addr_key) keep row with latest LODGEMENT_DATE
    con.execute(f"""
        COPY (
            WITH raw AS (
                SELECT
                    lmk_key,
                    upper(trim(address1)) AS address1,
                    upper(trim(address2)) AS address2,
                    upper(trim(address3)) AS address3,
                    regexp_replace(upper(trim(postcode)), '\\s+', ' ') AS postcode,
                    upper(trim(posttown)) AS posttown,
                    TRY_CAST(uprn AS BIGINT) AS uprn,
                    upper(trim(uprn_source)) AS uprn_source,
                    TRY_CAST(lodgement_date AS DATE) AS lodgement_date,
                    TRY_CAST(inspection_date AS DATE) AS inspection_date,
                    upper(trim(transaction_type)) AS transaction_type,
                    upper(trim(property_type)) AS property_type,
                    upper(trim(built_form)) AS built_form,
                    upper(trim(construction_age_band)) AS construction_age_band,
                    TRY_CAST(total_floor_area AS FLOAT) AS total_floor_area,
                    TRY_CAST(number_habitable_rooms AS SMALLINT) AS number_habitable_rooms,
                    TRY_CAST(number_heated_rooms AS SMALLINT) AS number_heated_rooms,
                    upper(trim(floor_level)) AS floor_level,
                    TRY_CAST(flat_storey_count AS SMALLINT) AS flat_storey_count,
                    upper(trim(flat_top_storey)) AS flat_top_storey,
                    TRY_CAST(extension_count AS SMALLINT) AS extension_count,
                    upper(trim(current_energy_rating)) AS current_energy_rating,
                    upper(trim(potential_energy_rating)) AS potential_energy_rating,
                    TRY_CAST(current_energy_efficiency AS SMALLINT) AS current_energy_efficiency,
                    TRY_CAST(potential_energy_efficiency AS SMALLINT) AS potential_energy_efficiency,
                    TRY_CAST(environment_impact_current AS SMALLINT) AS environment_impact_current,
                    TRY_CAST(co2_emissions_current AS FLOAT) AS co2_emissions_current,
                    TRY_CAST(co2_emiss_curr_per_floor_area AS FLOAT) AS co2_emiss_curr_per_floor_area,
                    TRY_CAST(energy_consumption_current AS FLOAT) AS energy_consumption_current,
                    upper(trim(mains_gas_flag)) AS mains_gas_flag,
                    upper(trim(main_fuel)) AS main_fuel,
                    upper(trim(solar_water_heating_flag)) AS solar_water_heating_flag,
                    upper(trim(mechanical_ventilation)) AS mechanical_ventilation,
                    upper(trim(tenure)) AS tenure,
                    TRY_CAST(report_type AS SMALLINT) AS report_type,
                    TRY_CAST(heating_cost_current AS FLOAT) AS heating_cost_current,
                    TRY_CAST(lighting_cost_current AS FLOAT) AS lighting_cost_current,
                    TRY_CAST(hot_water_cost_current AS FLOAT) AS hot_water_cost_current,
                    upper(trim(walls_description)) AS walls_description,
                    upper(trim(walls_energy_eff)) AS walls_energy_eff,
                    upper(trim(roof_description)) AS roof_description,
                    upper(trim(roof_energy_eff)) AS roof_energy_eff,
                    upper(trim(windows_description)) AS windows_description,
                    upper(trim(windows_energy_eff)) AS windows_energy_eff,
                    upper(trim(mainheat_description)) AS mainheat_description,
                    upper(trim(mainheat_energy_eff)) AS mainheat_energy_eff,
                    upper(trim(hotwater_description)) AS hotwater_description,
                    upper(trim(hot_water_energy_eff)) AS hot_water_energy_eff,
                    upper(trim(lighting_description)) AS lighting_description,
                    upper(trim(lighting_energy_eff)) AS lighting_energy_eff,
                    upper(trim(local_authority)) AS local_authority,
                    upper(trim(local_authority_label)) AS local_authority_label,
                    upper(trim(county)) AS county,
                    regexp_replace(
                        regexp_replace(
                            upper(trim(concat_ws(' ', address1, address2))),
                            '[^A-Z0-9 ]', ' ', 'g'
                        ),
                        '\\s+', ' ', 'g'
                    ) AS addr_key
                FROM read_csv(
                    '{EPC_DIR}/**/*.csv',
                    header=true,
                    union_by_name=true,
                    ignore_errors=true
                )
            ),
            ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY postcode, addr_key
                        ORDER BY lodgement_date DESC NULLS LAST
                    ) AS rn
                FROM raw
                WHERE postcode IS NOT NULL AND postcode != ''
            )
            SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1
        ) TO '{out}'
        (FORMAT PARQUET, CODEC 'ZSTD', COMPRESSION_LEVEL 6)
    """)

    count = con.execute(f"SELECT COUNT(*) FROM '{out}'").fetchone()[0]
    logger.success(f"stg_epc.parquet written — {count:,} rows (deduplicated)")


if __name__ == "__main__":
    main()

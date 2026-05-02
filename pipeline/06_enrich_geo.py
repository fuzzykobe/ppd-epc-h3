"""Join PPD + EPC + ONS, resolve EPC cert dates → transactions_geo.parquet"""
import click
import duckdb
from loguru import logger

from pipeline.config import DATA_STAGED, DUCKDB_MEMORY, DUCKDB_THREADS


@click.command()
def main() -> None:
    ppd = DATA_STAGED / "stg_ppd.parquet"
    epc = DATA_STAGED / "stg_epc.parquet"
    ons = DATA_STAGED / "stg_ons.parquet"
    matches = DATA_STAGED / "matches_all.parquet"
    out = DATA_STAGED / "transactions_geo.parquet"

    for p in (ppd, epc, ons, matches):
        if not p.exists():
            raise FileNotFoundError(f"{p} not found")

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    con.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY}'")

    logger.info("Joining PPD + matches + EPC + ONS")
    con.execute(f"""
        COPY (
            WITH matched AS (
                SELECT
                    p.transaction_id,
                    p.price,
                    p.transfer_date,
                    p.transfer_year,
                    p.transfer_month,
                    p.postcode,
                    p.property_type,
                    p.old_new,
                    p.duration,
                    p.paon,
                    p.saon,
                    p.street,
                    p.town_city,
                    p.district,
                    p.county AS county_ppd,
                    m.lmk_key,
                    m.match_type,
                    m.match_score
                FROM '{ppd}' p
                LEFT JOIN '{matches}' m USING (transaction_id)
            ),
            with_epc AS (
                SELECT
                    t.*,
                    e.lodgement_date AS epc_lodgement_date,
                    e.inspection_date AS epc_inspection_date,
                    e.property_type AS epc_property_type,
                    e.built_form AS epc_built_form,
                    e.construction_age_band,
                    e.total_floor_area,
                    e.number_habitable_rooms,
                    e.number_heated_rooms,
                    e.floor_level,
                    e.current_energy_rating,
                    e.current_energy_efficiency,
                    e.potential_energy_rating,
                    e.potential_energy_efficiency,
                    e.co2_emissions_current,
                    e.co2_emiss_curr_per_floor_area,
                    e.energy_consumption_current,
                    e.mains_gas_flag,
                    e.main_fuel,
                    e.solar_water_heating_flag,
                    e.mechanical_ventilation,
                    e.tenure AS tenure_epc,
                    e.report_type,
                    e.heating_cost_current,
                    e.lighting_cost_current,
                    e.hot_water_cost_current,
                    e.walls_description,
                    e.walls_energy_eff,
                    e.roof_description,
                    e.roof_energy_eff,
                    e.windows_description,
                    e.windows_energy_eff,
                    e.mainheat_description,
                    e.mainheat_energy_eff,
                    e.local_authority_label,
                    e.county AS county_epc,
                    CASE
                        WHEN e.lodgement_date IS NULL THEN NULL
                        WHEN e.lodgement_date <= t.transfer_date THEN TRUE
                        ELSE FALSE
                    END AS epc_before_transaction
                FROM matched t
                LEFT JOIN '{epc}' e USING (lmk_key)
            ),
            with_geo AS (
                SELECT
                    t.*,
                    o.lat,
                    o.lon,
                    o.oa21,
                    o.lsoa21,
                    o.msoa21,
                    o.ladcd,
                    o.ladnm,
                    o.rgn,
                    o.ctry
                FROM with_epc t
                LEFT JOIN '{ons}' o ON t.postcode = o.postcode
            )
            SELECT * FROM with_geo
        ) TO '{out}'
        (FORMAT PARQUET, CODEC 'ZSTD', COMPRESSION_LEVEL 6)
    """)

    count = con.execute(f"SELECT COUNT(*) FROM '{out}'").fetchone()[0]
    geo_count = con.execute(f"SELECT COUNT(*) FROM '{out}' WHERE lat IS NOT NULL").fetchone()[0]
    logger.success(
        f"transactions_geo.parquet written — {count:,} rows, "
        f"{100.0 * geo_count / count:.1f}% geo-enriched"
    )


if __name__ == "__main__":
    main()

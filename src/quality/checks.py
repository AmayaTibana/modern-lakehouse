from src.load.load_snowflake import connect

CHECKS = [
    ("pokemon count >= 151", "SELECT COUNT(*) FROM ANALYTICS.DIM_POKEMON", lambda v: v >= 151),
    (
        "pokemon ids unique",
        "SELECT COUNT(*) - COUNT(DISTINCT pokemon_id) FROM ANALYTICS.DIM_POKEMON",
        lambda v: v == 0,
    ),
    ("forecast fact has rows", "SELECT COUNT(*) FROM ANALYTICS.FACT_FORECAST", lambda v: v > 0),
    (
        "no null location keys",
        "SELECT COUNT(*) FROM ANALYTICS.FACT_FORECAST WHERE location_key IS NULL",
        lambda v: v == 0,
    ),
    (
        "every forecast has a condition",
        "SELECT COUNT(*) FROM ANALYTICS.FACT_FORECAST WHERE condition_key IS NULL",
        lambda v: v == 0,
    ),
    (
        "temperatures plausible (F)",
        "SELECT COUNT(*) FROM ANALYTICS.FACT_FORECAST WHERE temperature NOT BETWEEN -60 AND 130",
        lambda v: v == 0,
    ),
    (
        "every pokemon has a type",
        (
            "SELECT COUNT(*) FROM ANALYTICS.DIM_POKEMON p WHERE NOT EXISTS "
            "(SELECT 1 FROM ANALYTICS.BRIDGE_POKEMON_TYPE b WHERE b.pokemon_id = p.pokemon_id)"
        ),
        lambda v: v == 0,
    ),
]


def run() -> None:
    conn = connect()
    failures = []
    try:
        cursor = conn.cursor()
        for name, sql, is_ok in CHECKS:
            value = cursor.execute(sql).fetchone()[0]
            status = "PASS" if is_ok(value) else "FAIL"
            print(f"[{status}] {name} (value={value})")
            if status == "FAIL":
                failures.append(name)
    finally:
        conn.close()
    if failures:
        raise RuntimeError(f"Data quality checks failed: {failures}")


if __name__ == "__main__":
    run()
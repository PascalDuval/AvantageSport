"""
Vérification DuckDB — Delta Lake Gold

Script de vérification rapide de la couche Gold.
Référencé dans README_ROUND3.md.

Usage:
    python scripts/check_gold_delta.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def main():
    import duckdb
    from config import BRONZE_DIR

    gold_path = str(BRONZE_DIR.parent / "gold" / "avantages")

    if not Path(gold_path).exists():
        print(f"❌ Delta Lake Gold non trouvé : {gold_path}")
        print("   Lancez d'abord : python scripts/run_round3.py")
        sys.exit(1)

    print(f"🦆 DuckDB — lecture Delta Lake Gold")
    print(f"   Chemin : {gold_path}")
    print()

    conn = duckdb.connect()
    conn.execute("INSTALL delta; LOAD delta;")

    # Vue d'ensemble
    df_global = conn.execute(f"""
        SELECT
            COUNT(*)                                                AS nb_salaries,
            SUM(CASE WHEN eligible_prime THEN 1 ELSE 0 END)        AS nb_eligible_prime,
            ROUND(SUM(montant_prime)::DECIMAL, 2)                  AS cout_total_primes,
            SUM(CASE WHEN eligible_jours_be THEN 1 ELSE 0 END)     AS nb_eligible_be,
            SUM(nb_jours_bienetre)                                  AS total_jours_accordes,
            ROUND(AVG(nb_activites_12m)::DECIMAL, 1)               AS moy_activites,
            MAX(nb_activites_12m)                                   AS max_activites,
            MAX(params_version)                                     AS version
        FROM delta_scan('{gold_path}')
    """).fetchdf()

    print("📊 Vue d'ensemble Gold :")
    for col, val in df_global.iloc[0].items():
        print(f"   {col:<30} : {val}")

    # Top 5 primes
    df_top = conn.execute(f"""
        SELECT employee_id,
               ROUND(montant_prime::DECIMAL, 2)    AS prime_euro,
               nb_activites_12m,
               eligible_jours_be
        FROM delta_scan('{gold_path}')
        WHERE eligible_prime = TRUE
        ORDER BY montant_prime DESC
        LIMIT 5
    """).fetchdf()

    print("\n🏆 Top 5 primes :")
    print(df_top.to_string(index=False))

    # Distribution activités
    df_acts = conn.execute(f"""
        SELECT
            CASE
                WHEN nb_activites_12m = 0       THEN '0 activité'
                WHEN nb_activites_12m < 5        THEN '1-4 activités'
                WHEN nb_activites_12m < 15       THEN '5-14 activités'
                WHEN nb_activites_12m < 30       THEN '15-29 activités (éligibles BE)'
                ELSE                                  '30+ activités'
            END AS tranche,
            COUNT(*) AS nb_salaries
        FROM delta_scan('{gold_path}')
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchdf()

    print("\n📈 Distribution des activités :")
    print(df_acts.to_string(index=False))

    conn.close()
    print(f"\n✅ Delta Lake Gold lisible — {gold_path}")


if __name__ == "__main__":
    main()

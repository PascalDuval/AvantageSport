"""
Diagnostic Gold — pourquoi les primes sont à 0 ?

Vérifie l'état de la table employees (PostgreSQL) et simule le calcul
que fait etl_gold.py pour localiser le problème.

Usage:
    python scripts/diagnose_gold.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database import get_connection, get_config, get_engine
import pandas as pd


SEP = "─" * 60


def main():
    print(f"\n{'='*60}")
    print("  DIAGNOSTIC GOLD — état de la table employees")
    print(f"{'='*60}\n")

    engine = get_engine()

    # ── 1. État global de la table employees ─────────────────────
    print(f"{SEP}")
    print("  [1/4] État global — employees (PostgreSQL)")
    print(SEP)
    df_check = pd.read_sql("""
        SELECT
            COUNT(*)                                            AS total,
            SUM(CASE WHEN eligible_prime THEN 1 ELSE 0 END)    AS nb_eligible,
            SUM(CASE WHEN salaire_brut IS NULL THEN 1 ELSE 0 END) AS null_salaire,
            ROUND(AVG(salaire_brut)::NUMERIC, 2)               AS avg_salaire,
            ROUND(MIN(salaire_brut)::NUMERIC, 2)               AS min_salaire,
            ROUND(MAX(salaire_brut)::NUMERIC, 2)               AS max_salaire
        FROM employees
    """, engine)
    for col, val in df_check.iloc[0].items():
        print(f"   {col:<25} : {val}")

    # ── 2. Répartition mode × eligible_prime ─────────────────────
    print(f"\n{SEP}")
    print("  [2/4] Répartition mode × eligible_prime × salaire_brut nul")
    print(SEP)
    df_modes = pd.read_sql("""
        SELECT
            mode_deplacement,
            eligible_prime,
            COUNT(*)                                               AS nb,
            SUM(CASE WHEN salaire_brut IS NULL THEN 1 ELSE 0 END) AS null_sal,
            ROUND(AVG(distance_bureau_km)::NUMERIC, 1)            AS dist_moy
        FROM employees
        GROUP BY mode_deplacement, eligible_prime
        ORDER BY mode_deplacement, eligible_prime
    """, engine)
    print(df_modes.to_string(index=False))

    # ── 3. Simulation du calcul Gold ─────────────────────────────
    print(f"\n{SEP}")
    print("  [3/4] Simulation build_gold_df — mêmes données que l'ETL")
    print(SEP)
    df_emp = pd.read_sql("""
        SELECT id AS employee_id, nom, prenom, bu,
               salaire_brut, eligible_prime, mode_deplacement, sport_declare
        FROM employees
    """, engine)

    taux = float(get_config("taux_prime", "0.05"))
    print(f"   taux_prime (config) = {taux}")
    print(f"   nb lignes lues      = {len(df_emp)}")
    print(f"   eligible_prime sum  = {df_emp['eligible_prime'].sum()}")
    print(f"   salaire_brut NaN    = {df_emp['salaire_brut'].isna().sum()}")

    df_emp["montant_prime"] = df_emp.apply(
        lambda r: round(float(r["salaire_brut"]) * taux, 2)
        if r["eligible_prime"] and pd.notna(r["salaire_brut"])
        else 0.0,
        axis=1,
    )
    nb_avec_prime = (df_emp["montant_prime"] > 0).sum()
    total_primes  = df_emp["montant_prime"].sum()
    print(f"\n   → Salariés avec prime > 0  : {nb_avec_prime}")
    print(f"   → Coût total primes         : {total_primes:,.2f} €")

    if nb_avec_prime == 0:
        print("\n  ❌ PROBLÈME DÉTECTÉ :")
        nb_elig = int(df_emp["eligible_prime"].sum())
        nb_null_sal = int(df_emp["salaire_brut"].isna().sum())
        if nb_elig == 0:
            print("     eligible_prime = FALSE pour TOUS les salariés dans PostgreSQL.")
            print("     → La table employees (PostgreSQL) est désynchronisée du Delta Lake Silver.")
            print("     → SOLUTION : relancez python scripts/run_round2.py")
        elif nb_null_sal == nb_elig:
            print(f"     {nb_elig} éligibles mais salaire_brut IS NULL pour eux → prime = 0.")
            print("     → SOLUTION : vérifiez que raw_rh contient bien les salaires.")
        else:
            print(f"     {nb_elig} éligibles, {nb_null_sal} avec salaire NULL.")
            print("     Cas mixte — voir détail ci-dessous.")
    else:
        print(f"\n  ✅ Calcul OK — {nb_avec_prime} primes calculées, total = {total_primes:,.2f} €")

    # ── 4. Détail des éligibles ──────────────────────────────────
    print(f"\n{SEP}")
    print("  [4/4] Détail — salariés eligible_prime=TRUE")
    print(SEP)
    df_elig = df_emp[df_emp["eligible_prime"] == True][
        ["employee_id", "nom", "prenom", "mode_deplacement", "salaire_brut", "montant_prime"]
    ].head(20)
    if df_elig.empty:
        print("   Aucun salarié avec eligible_prime = TRUE dans PostgreSQL.")
        print("   → C'est le problème : relancez run_round2.py pour repeupler employees.")
    else:
        print(df_elig.to_string(index=False))

    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()

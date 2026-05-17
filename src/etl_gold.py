"""
ETL Gold — Round 3

Calcule les avantages sportifs pour chaque salarié et peuple la table
`avantages_calcules` (couche Gold), puis écrit en Delta Lake Gold.

Deux avantages calculés :
  ① Prime sportive 5 % : salaire_brut × taux_prime
       → pour les salariés avec eligible_prime = True (calculé Round 2)
  ② 5 journées bien-être :
       → pour les salariés ayant ≥ min_activites_be activités
          dans la fenêtre des <fenetre_activites_j> derniers jours
          (calculée depuis le MAX de strava_activities, pas NOW())

Tous les paramètres sont lus depuis la table `config` en base.
La table est rechargée à chaque run (TRUNCATE + INSERT) pour permettre
le recalcul complet si un paramètre change (ex : taux_prime modifié).

Usage:
    python src/etl_gold.py
    python src/etl_gold.py --dry-run
    python src/etl_gold.py --params-version v2.0_taux7pct
"""
import argparse
import logging
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent))
from database import get_connection, get_config, get_engine, log_run, ping

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/etl_gold.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════
# 1. Lecture des paramètres métier
# ════════════════════════════════════════════════════════════════

def load_params() -> dict:
    """
    Charge les paramètres métier depuis la table `config` en base.

    Toujours lus depuis la base, jamais hardcodés, pour que
    Power BI puisse rejouer le calcul si un taux change.

    Returns:
        dict avec les clés :
            taux_prime       (float)
            min_activites_be (int)
            nb_jours_be      (int)
            fenetre_j        (int) : jours de la fenêtre de comptage
            params_version   (str)
    """
    params = {
        "taux_prime":       float(get_config("taux_prime",         "0.05")),
        "min_activites_be": int(get_config("min_activites_be",     "15")),
        "nb_jours_be":      int(get_config("nb_jours_bienetre",    "5")),
        "fenetre_j":        int(get_config("fenetre_activites_j",  "365")),
        "params_version":   get_config("params_version",           "v1.0"),
    }
    logger.info("⚙️  Paramètres métier chargés depuis config :")
    for k, v in params.items():
        logger.info(f"   {k:<22} = {v}")
    return params


# ════════════════════════════════════════════════════════════════
# 2. Comptage des activités par salarié
# ════════════════════════════════════════════════════════════════

def count_activities_per_employee(fenetre_j: int) -> pd.DataFrame:
    """
    Compte les activités de chaque salarié dans la fenêtre temporelle.

    Fenêtre = [MAX(date_debut) - fenetre_j jours, MAX(date_debut)].
    On utilise MAX plutôt que NOW() car les données Round 1 sont
    toutes en 2025 — si on lance ce script en 2026, NOW()-365 ne
    capturerait rien.

    Args:
        fenetre_j: nombre de jours à considérer (config: fenetre_activites_j).

    Returns:
        DataFrame avec colonnes : employee_id, nb_activites_12m, date_ref.
    """
    sql = f"""
        WITH date_ref AS (
            SELECT MAX(date_debut) AS ref_date FROM strava_activities
        )
        SELECT
            sa.employee_id,
            COUNT(*)                    AS nb_activites_12m,
            dr.ref_date                 AS date_ref
        FROM strava_activities sa
        CROSS JOIN date_ref dr
        WHERE sa.date_debut >= dr.ref_date - INTERVAL '{fenetre_j} days'
        GROUP BY sa.employee_id, dr.ref_date
    """
    engine = get_engine()
    df = pd.read_sql(sql, engine)
    logger.info(
        f"📊 Activités comptées : {len(df)} salariés actifs "
        f"(fenêtre = {fenetre_j} jours depuis MAX strava_activities)"
    )
    return df


# ════════════════════════════════════════════════════════════════
# 3. Calcul des avantages
# ════════════════════════════════════════════════════════════════

def build_gold_df(params: dict) -> pd.DataFrame:
    """
    Construit le DataFrame Gold complet pour tous les salariés.

    Sources :
      - `employees`        : salaire_brut, eligible_prime (Silver)
      - `strava_activities`: comptage activités (Round 1)

    Calculs :
      - montant_prime      = salaire_brut * taux_prime  (si eligible_prime)
      - eligible_jours_be  = nb_activites_12m >= min_activites_be
      - nb_jours_bienetre  = nb_jours_be               (si eligible_jours_be)

    Args:
        params: dict retourné par load_params().

    Returns:
        DataFrame avec une ligne par salarié (161 lignes attendues).
    """
    engine = get_engine()

    # ── Lecture employees (Silver) ───────────────────────────────
    logger.info("📖 Lecture table employees (Silver)...")
    df_emp = pd.read_sql(
        """
        SELECT id AS employee_id, nom, prenom, bu,
               salaire_brut, eligible_prime, mode_deplacement, sport_declare
        FROM employees
        """,
        engine
    )
    logger.info(f"   {len(df_emp)} salariés chargés")
    nb_eligible = df_emp["eligible_prime"].sum()
    logger.info(f"   Éligibles prime : {nb_eligible}")

    # ── Comptage activités ───────────────────────────────────────
    df_acts = count_activities_per_employee(params["fenetre_j"])

    # ── Jointure ─────────────────────────────────────────────────
    df = df_emp.merge(
        df_acts[["employee_id", "nb_activites_12m"]],
        on="employee_id",
        how="left"
    )
    df["nb_activites_12m"] = df["nb_activites_12m"].fillna(0).astype(int)

    # ── Calcul prime ─────────────────────────────────────────────
    df["montant_prime"] = df.apply(
        lambda r: round(float(r["salaire_brut"]) * params["taux_prime"], 2)
        if r["eligible_prime"] and pd.notna(r["salaire_brut"])
        else 0.0,
        axis=1,
    )

    # ── Calcul journées bien-être ────────────────────────────────
    df["eligible_jours_be"] = df["nb_activites_12m"] >= params["min_activites_be"]
    df["nb_jours_bienetre"] = df["eligible_jours_be"].apply(
        lambda e: params["nb_jours_be"] if e else 0
    )

    # ── Métadonnées ──────────────────────────────────────────────
    df["date_calcul"]     = date.today()
    df["params_version"]  = params["params_version"]

    # ── Résumé ──────────────────────────────────────────────────
    nb_prime  = int(df["eligible_prime"].sum())
    nb_be     = int(df["eligible_jours_be"].sum())
    total_p   = round(float(df["montant_prime"].sum()), 2)
    logger.info(f"\n  📊 Résultats Gold :")
    logger.info(f"     Éligibles prime     : {nb_prime} / {len(df)}")
    logger.info(f"     Coût total primes   : {total_p:,.2f} €")
    logger.info(f"     Éligibles jours BE  : {nb_be} / {len(df)}")
    logger.info(f"     Params version      : {params['params_version']}")

    return df


# ════════════════════════════════════════════════════════════════
# 4. Écriture PostgreSQL
# ════════════════════════════════════════════════════════════════

def write_avantages(df: pd.DataFrame, dry_run: bool = False) -> int:
    """
    Écrit les avantages calculés dans la table `avantages_calcules`.

    Stratégie : TRUNCATE + INSERT complet (idempotent).
    Justification : si le taux ou les activités changent,
    tous les avantages doivent être recalculés ensemble.

    Args:
        df:      DataFrame Gold construit par build_gold_df().
        dry_run: si True, affiche sans insérer.

    Returns:
        int: nombre de lignes insérées.
    """
    if dry_run:
        logger.info(f"🧪 DRY RUN avantages_calcules — {len(df)} lignes (non insérées)")
        cols_show = ["employee_id", "montant_prime", "eligible_prime",
                     "eligible_jours_be", "nb_activites_12m", "nb_jours_bienetre"]
        logger.info(df[[c for c in cols_show if c in df.columns]].head(10).to_string())
        return 0

    rows = []
    for _, r in df.iterrows():
        rows.append((
            str(r["employee_id"]),
            float(r["montant_prime"]) if pd.notna(r["montant_prime"]) else 0.0,
            bool(r["eligible_prime"]),
            bool(r["eligible_jours_be"]),
            int(r["nb_activites_12m"]),
            int(r["nb_jours_bienetre"]),
            r["date_calcul"],
            str(r["params_version"]),
        ))

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Idempotent : on repart de zéro à chaque run
            cur.execute("TRUNCATE TABLE avantages_calcules RESTART IDENTITY")
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO avantages_calcules
                    (employee_id, montant_prime, eligible_prime, eligible_jours_be,
                     nb_activites_12m, nb_jours_bienetre, date_calcul, params_version)
                VALUES %s
                """,
                rows,
            )

    logger.info(f"✅ avantages_calcules : {len(rows)} lignes insérées")
    return len(rows)


# ════════════════════════════════════════════════════════════════
# 5. Écriture Delta Lake Gold
# ════════════════════════════════════════════════════════════════

def write_gold_delta(df: pd.DataFrame) -> None:
    """
    Écrit le DataFrame Gold en Delta Lake.

    Chemin : data/delta/gold/avantages/

    Colonnes retenues : employee_id, montant_prime, eligible_prime,
    eligible_jours_be, nb_activites_12m, nb_jours_bienetre,
    date_calcul, params_version.

    Args:
        df: DataFrame Gold complet.
    """
    from deltalake.writer import write_deltalake
    from config import BRONZE_DIR

    gold_path = BRONZE_DIR.parent / "gold" / "avantages"
    gold_path.mkdir(parents=True, exist_ok=True)

    cols = [
        "employee_id", "montant_prime", "eligible_prime",
        "eligible_jours_be", "nb_activites_12m", "nb_jours_bienetre",
        "date_calcul", "params_version",
    ]
    df_out = df[[c for c in cols if c in df.columns]].copy()

    # date → string (Delta Lake ne gère pas date nativement sans PyArrow)
    df_out["date_calcul"] = df_out["date_calcul"].apply(
        lambda x: str(x) if x is not None else None
    )
    # Assurer les types
    df_out["montant_prime"]    = df_out["montant_prime"].astype(float)
    df_out["nb_activites_12m"] = df_out["nb_activites_12m"].astype(int)
    df_out["nb_jours_bienetre"]= df_out["nb_jours_bienetre"].astype(int)

    write_deltalake(str(gold_path), df_out, mode="overwrite")
    logger.info(f"🗃  Delta Lake Gold écrit : {gold_path}")


# ════════════════════════════════════════════════════════════════
# 6. Rapport final Gold
# ════════════════════════════════════════════════════════════════

def print_gold_stats() -> None:
    """Affiche un bilan complet de la couche Gold depuis PostgreSQL."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN eligible_prime THEN 1 ELSE 0 END) AS nb_prime,
                    SUM(montant_prime) AS cout_total_primes,
                    SUM(CASE WHEN eligible_jours_be THEN 1 ELSE 0 END) AS nb_be,
                    SUM(nb_jours_bienetre) AS total_jours_be,
                    MAX(params_version) AS version,
                    MAX(date_calcul) AS date_calcul
                FROM avantages_calcules
            """)
            g = cur.fetchone()

            # Ventilation par BU
            cur.execute("""
                SELECT e.bu,
                       COUNT(*) AS nb_salaries,
                       SUM(CASE WHEN a.eligible_prime THEN 1 ELSE 0 END) AS nb_prime,
                       ROUND(SUM(a.montant_prime)::NUMERIC, 2) AS cout_prime,
                       SUM(CASE WHEN a.eligible_jours_be THEN 1 ELSE 0 END) AS nb_be
                FROM avantages_calcules a
                JOIN employees e ON e.id = a.employee_id
                GROUP BY e.bu ORDER BY cout_prime DESC NULLS LAST
            """)
            by_bu = cur.fetchall()

            # Top 10 primes
            cur.execute("""
                SELECT e.nom, e.prenom, e.bu, e.mode_deplacement,
                       ROUND(e.distance_bureau_km::NUMERIC, 1) AS km,
                       a.montant_prime
                FROM avantages_calcules a
                JOIN employees e ON e.id = a.employee_id
                WHERE a.eligible_prime = TRUE
                ORDER BY a.montant_prime DESC
                LIMIT 10
            """)
            top_primes = cur.fetchall()

    print(f"\n{'═'*65}")
    print(f"  💰 GOLD — AVANTAGES CALCULES (version : {g['version']})")
    print(f"  Date de calcul : {g['date_calcul']}")
    print(f"{'═'*65}")
    print(f"\n  Prime sportive :")
    print(f"    Éligibles    : {g['nb_prime']} / {g['total']} salariés")
    print(f"    Coût total   : {g['cout_total_primes']:,.2f} €")

    print(f"\n  Journées bien-être :")
    print(f"    Éligibles    : {g['nb_be']} / {g['total']} salariés")
    print(f"    Total jours  : {g['total_jours_be']} jours accordés")

    print(f"\n  Ventilation par BU :")
    print(f"  {'BU':<15} {'Salariés':>9} {'Prime':>7} {'€ prime':>12} {'Jours BE':>10}")
    print(f"  {'-'*57}")
    for b in by_bu:
        print(
            f"  {str(b['bu']):<15} {b['nb_salaries']:>9} {b['nb_prime']:>7} "
            f"{float(b['cout_prime'] or 0):>12,.2f} {b['nb_be']:>10}"
        )

    print(f"\n  Top 10 primes :")
    for r in top_primes:
        print(
            f"    {str(r['prenom'])[:10]:<10} {str(r['nom'])[:12]:<12} "
            f"({str(r['bu']):<8}) {float(r['km'] or 0):>5.1f} km → "
            f"{float(r['montant_prime']):>8,.2f} €"
        )
    print(f"{'═'*65}\n")


# ════════════════════════════════════════════════════════════════
# 7. Point d'entrée
# ════════════════════════════════════════════════════════════════

def main(dry_run: bool = False, params_version_override: Optional[str] = None) -> dict:
    """
    Orchestre l'ETL Gold complet.

    Args:
        dry_run:                  si True, calcule mais n'écrit pas.
        params_version_override:  surcharge la version de params (ex: 'v2.0').

    Returns:
        dict avec les métriques Gold.
    """
    debut = datetime.now()

    if not ping():
        sys.exit(1)

    logger.info("=" * 65)
    logger.info("💰 ETL Gold — Round 3")
    logger.info(f"   dry_run={dry_run}")
    logger.info("=" * 65)

    # Vérifier que Silver est peuplé
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM employees")
            nb_emp = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM strava_activities")
            nb_acts = cur.fetchone()[0]

    if nb_emp == 0:
        logger.error("La table employees est vide. Lancez d'abord run_round2.py")
        sys.exit(1)
    if nb_acts == 0:
        logger.error("strava_activities est vide. Lancez d'abord run_round1.py")
        sys.exit(1)

    logger.info(f"   employees     : {nb_emp} lignes")
    logger.info(f"   strava_acts   : {nb_acts} lignes")

    # Paramètres
    params = load_params()
    if params_version_override:
        params["params_version"] = params_version_override
        logger.info(f"   Version surchargée : {params_version_override}")

    # Calcul Gold
    df_gold = build_gold_df(params)

    # Écriture
    nb = write_avantages(df_gold, dry_run=dry_run)

    if not dry_run:
        write_gold_delta(df_gold)
        print_gold_stats()
        log_run(
            flow_name="etl_gold",
            etape="avantages_calcules + Delta Lake Gold",
            statut="SUCCESS",
            debut=debut,
            nb_lignes=nb,
            metadata={
                "nb_eligible_prime":  int(df_gold["eligible_prime"].sum()),
                "nb_eligible_be":     int(df_gold["eligible_jours_be"].sum()),
                "cout_total_primes":  float(df_gold["montant_prime"].sum()),
                "params_version":     params["params_version"],
            },
        )

    return {
        "nb_eligible_prime":  int(df_gold["eligible_prime"].sum()),
        "nb_eligible_be":     int(df_gold["eligible_jours_be"].sum()),
        "cout_total_primes":  float(df_gold["montant_prime"].sum()),
        "nb_total":           len(df_gold),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Gold — calcul primes + journées BE")
    parser.add_argument("--dry-run", action="store_true",
                        help="Calcule sans écrire en base")
    parser.add_argument("--params-version", type=str, default=None,
                        help="Surcharge la version des paramètres (ex: v2.0_taux7pct)")
    args = parser.parse_args()
    main(dry_run=args.dry_run, params_version_override=args.params_version)

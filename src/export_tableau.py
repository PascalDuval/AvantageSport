"""
Export Tableau — Round 5

Deux modes d'export (progression v1 → v2) :

  v1 (statique)  : CSV  → reports/tableau/*.csv      (7 fichiers, UTF-8 BOM)
                   Excel → reports/tableau/*.xlsx      (classeur multi-onglets)
  v2 (dynamique) : Hyper → reports/tableau/*.hyper    (extract natif Tableau)

La v2 utilise tableauhyperapi pour générer un fichier .hyper lisible
directement par Tableau Desktop sans aucune manipulation manuelle :
  - Tableau Desktop → Se connecter → Fichiers supplémentaires → *.hyper
  - Refresh automatisable depuis Python (Kestra déclenche le script)
  - Format columnar optimisé, plus rapide que CSV pour des datasets > 50 k lignes

Pourquoi Tableau plutôt que Power BI ?
  - Connexion PostgreSQL native (pas de driver ODBC tiers)
  - Hyper API Python officielle (tableauhyperapi) pour l'injection directe
  - Tableau Server / Cloud pour la mise en production (vs Power BI Service)
  - Pas de dépendance Microsoft dans un contexte data engineering

Datasets générés (7) :
  1. vue_globale.csv / sheet      → Vue Globale (KPI synthèse + BU)
  2. primes_sportives.csv / sheet → Primes Sportives (liste éligibles)
  3. journees_bienetre.csv / sheet→ Journées Bien-être (activités BE)
  4. activites_sportives.csv / sheet→ Activités Sportives (volumes + sports)
  5. anomalies_qualite.csv / sheet→ Anomalies & Qualité (contrôles SODA)
  6. config_params.csv / sheet    → Paramètres (pour les filtres Tableau)
  7. pipeline_logs.csv / sheet    → Logs pipeline (monitoring)

Usage:
    python src/export_tableau.py                         # CSV (défaut, v1)
    python src/export_tableau.py --format excel          # Excel (v1)
    python src/export_tableau.py --format hyper          # Hyper extract (v2)
    python src/export_tableau.py --output-dir mon/dossier
"""
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from database import get_engine, log_run, ping

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/export_tableau.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("reports/tableau")

# Détection optionnelle de tableauhyperapi (v2 uniquement)
try:
    from tableauhyperapi import (
        HyperProcess, Telemetry, Connection, CreateMode,
        TableDefinition, TableName, SchemaName, SqlType, Inserter,
    )
    HYPER_AVAILABLE = True
except ImportError:
    HYPER_AVAILABLE = False


# ════════════════════════════════════════════════════════════════
# Requêtes SQL — une par feuille Tableau
# ════════════════════════════════════════════════════════════════

QUERIES: dict[str, str] = {

    # ── Feuille 1 : Vue Globale ──────────────────────────────────
    "vue_globale": """
        SELECT
            e.bu                                                          AS bu,
            COUNT(*)                                                      AS nb_salaries,
            SUM(CASE WHEN a.eligible_prime    THEN 1 ELSE 0 END)         AS nb_eligible_prime,
            ROUND(SUM(a.montant_prime)::NUMERIC, 2)                      AS cout_prime_euros,
            SUM(CASE WHEN a.eligible_jours_be THEN 1 ELSE 0 END)        AS nb_eligible_be,
            SUM(a.nb_jours_bienetre)                                      AS total_jours_be,
            MAX(a.params_version)                                         AS params_version,
            MAX(a.date_calcul)                                            AS date_calcul
        FROM avantages_calcules a
        JOIN employees e ON e.id = a.employee_id
        GROUP BY e.bu
        ORDER BY cout_prime_euros DESC
    """,

    # ── Feuille 2 : Primes Sportives ─────────────────────────────
    "primes_sportives": """
        SELECT
            e.id                                                          AS id_salarie,
            e.nom                                                         AS nom,
            e.prenom                                                      AS prenom,
            e.bu                                                          AS bu,
            e.type_contrat                                                AS type_contrat,
            e.mode_deplacement                                            AS mode_deplacement,
            ROUND(e.distance_bureau_km::NUMERIC, 2)                      AS distance_bureau_km,
            e.salaire_brut                                                AS salaire_brut,
            a.eligible_prime                                              AS eligible_prime,
            ROUND(a.montant_prime::NUMERIC, 2)                           AS montant_prime_euros,
            a.nb_activites_12m                                            AS nb_activites_12m,
            a.eligible_jours_be                                           AS eligible_jours_be,
            a.nb_jours_bienetre                                           AS nb_jours_be,
            a.params_version                                              AS params_version
        FROM avantages_calcules a
        JOIN employees e ON e.id = a.employee_id
        ORDER BY a.montant_prime DESC, e.nom
    """,

    # ── Feuille 3 : Journées Bien-être ────────────────────────────
    "journees_bienetre": """
        SELECT
            e.id                                                          AS id_salarie,
            e.nom                                                         AS nom,
            e.prenom                                                      AS prenom,
            e.bu                                                          AS bu,
            e.sport_declare                                               AS sport_declare,
            a.nb_activites_12m                                            AS nb_activites_12m,
            a.eligible_jours_be                                           AS eligible_jours_be,
            a.nb_jours_bienetre                                           AS nb_jours_be,
            CASE
                WHEN a.nb_activites_12m >= 25 THEN 'Très actif (25+)'
                WHEN a.nb_activites_12m >= 15 THEN 'Actif (15-24) — éligible BE'
                WHEN a.nb_activites_12m >= 5  THEN 'Occasionnel (5-14)'
                ELSE 'Peu actif (0-4)'
            END                                                           AS tranche_activite,
            a.params_version                                              AS params_version
        FROM avantages_calcules a
        JOIN employees e ON e.id = a.employee_id
        ORDER BY a.nb_activites_12m DESC
    """,

    # ── Feuille 4 : Activités Sportives ──────────────────────────
    "activites_sportives": """
        SELECT
            sa.employee_id                                                AS id_salarie,
            e.nom                                                         AS nom,
            e.prenom                                                      AS prenom,
            e.bu                                                          AS bu,
            sa.sport_type                                                 AS sport,
            DATE_TRUNC('month', sa.date_debut)::date                     AS mois,
            EXTRACT(YEAR  FROM sa.date_debut)::int                       AS annee,
            EXTRACT(MONTH FROM sa.date_debut)::int                       AS num_mois,
            TO_CHAR(sa.date_debut, 'Mon YYYY')                           AS label_mois,
            COUNT(*)                                                      AS nb_activites,
            ROUND(AVG(sa.distance_m / 1000.0)::NUMERIC, 2)              AS distance_moy_km,
            ROUND(AVG(sa.duree_s    / 60.0  )::NUMERIC, 1)              AS duree_moy_min,
            ROUND(SUM(sa.distance_m / 1000.0)::NUMERIC, 2)              AS distance_tot_km,
            sa.source                                                     AS source
        FROM strava_activities sa
        LEFT JOIN employees e ON e.id = sa.employee_id
        GROUP BY
            sa.employee_id, e.nom, e.prenom, e.bu, sa.sport_type,
            DATE_TRUNC('month', sa.date_debut),
            EXTRACT(YEAR FROM sa.date_debut), EXTRACT(MONTH FROM sa.date_debut),
            TO_CHAR(sa.date_debut, 'Mon YYYY'), sa.source
        ORDER BY mois, sport
    """,

    # ── Feuille 5 : Anomalies & Qualité ─────────────────────────
    "anomalies_qualite": """
        WITH last_run AS (
            SELECT MAX(run_at) AS last_run_at
            FROM data_quality_results
        )
        SELECT
            dqr.run_id                                                    AS run_id,
            dqr.run_at                                                    AS date_run,
            dqr.suite_name                                                AS suite,
            dqr.regle                                                     AS regle,
            dqr.table_cible                                               AS table_cible,
            dqr.colonne                                                   AS colonne,
            dqr.severite                                                  AS severite,
            dqr.resultat                                                  AS resultat_ok,
            CASE WHEN dqr.resultat THEN 'OK'
                 WHEN dqr.severite = 'BLOQUANT' THEN 'BLOQUANT'
                 ELSE 'WARNING' END                                       AS statut,
            dqr.detail                                                    AS detail,
            (dqr.run_at = lr.last_run_at)                                AS dernier_run
        FROM data_quality_results dqr
        CROSS JOIN last_run lr
        ORDER BY dqr.run_at DESC, dqr.severite DESC
    """,

    # ── Paramètres config (filtres Tableau) ─────────────────────
    "config_params": """
        SELECT
            cle         AS parametre,
            valeur      AS valeur,
            description AS description,
            updated_at  AS mis_a_jour
        FROM config
        ORDER BY cle
    """,

    # ── Logs pipeline (monitoring) ───────────────────────────────
    "pipeline_logs": """
        SELECT
            id          AS id,
            flow_name   AS flow,
            etape       AS etape,
            statut      AS statut,
            debut       AS debut,
            fin         AS fin,
            duree_ms    AS duree_ms,
            nb_lignes   AS nb_lignes,
            erreur      AS erreur
        FROM pipeline_runs
        ORDER BY debut DESC
        LIMIT 200
    """,
}


# ════════════════════════════════════════════════════════════════
# v1 — Export CSV (statique, fallback universel)
# ════════════════════════════════════════════════════════════════

def export_to_csv(output_dir: Path = OUTPUT_DIR) -> dict[str, int]:
    """
    v1 : Exporte chaque dataset dans un fichier CSV séparé.

    Encodage UTF-8 BOM (utf-8-sig) pour compatibilité Excel Windows.
    Destination : reports/tableau/*.csv

    Returns:
        dict {nom_dataset: nb_lignes}
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    engine = get_engine()
    results = {}

    for name, sql in QUERIES.items():
        logger.info(f"  📊 CSV {name}...")
        df = pd.read_sql(sql.strip(), engine)
        path = output_dir / f"{name}.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")
        results[name] = len(df)
        logger.info(f"     {len(df)} lignes → {path}")

    return results


# ════════════════════════════════════════════════════════════════
# v1 — Export Excel (snapshot multi-onglets)
# ════════════════════════════════════════════════════════════════

def export_to_excel(output_dir: Path = OUTPUT_DIR) -> Path:
    """
    v1 : Exporte tous les datasets dans un classeur Excel multi-onglets.

    Un onglet par dataset. Tableau Desktop peut lire ce fichier comme
    source de données "Microsoft Excel".

    Returns:
        Path du fichier .xlsx créé.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    engine = get_engine()
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    path = output_dir / f"poc_avantages_sportifs_{ts}.xlsx"

    sheet_names = {
        "vue_globale":          "01_Vue_Globale",
        "primes_sportives":     "02_Primes",
        "journees_bienetre":    "03_Journées_BE",
        "activites_sportives":  "04_Activités",
        "anomalies_qualite":    "05_Anomalies",
        "config_params":        "Config",
        "pipeline_logs":        "Logs_Pipeline",
    }

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, sheet_name in sheet_names.items():
            if name not in QUERIES:
                continue
            logger.info(f"  📋 Onglet '{sheet_name}'...")
            df = pd.read_sql(QUERIES[name].strip(), engine)
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
            logger.info(f"     {len(df)} lignes")

    logger.info(f"✅ Classeur Excel : {path}")
    return path


# ════════════════════════════════════════════════════════════════
# v2 — Export Hyper (inject dynamique dans Tableau)
# ════════════════════════════════════════════════════════════════

def _pandas_dtype_to_sql_type(dtype) -> "SqlType":
    """Mappe un dtype pandas vers un SqlType Hyper."""
    import numpy as np
    if pd.api.types.is_integer_dtype(dtype):
        return SqlType.int()
    if pd.api.types.is_float_dtype(dtype):
        return SqlType.double()
    if pd.api.types.is_bool_dtype(dtype):
        return SqlType.bool()
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return SqlType.timestamp_tz()
    return SqlType.text()


def _to_python(val):
    """Convertit les types numpy/pandas en types Python natifs pour l'Inserter Hyper."""
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    try:
        import numpy as np
        if isinstance(val, np.integer):
            return int(val)
        if isinstance(val, np.floating):
            return None if pd.isna(val) else float(val)
        if isinstance(val, np.bool_):
            return bool(val)
    except Exception:
        pass
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime()
    if not isinstance(val, (int, float, bool, str)):
        return str(val)
    return val


def export_to_hyper(output_dir: Path = OUTPUT_DIR) -> Path:
    """
    v2 : Génère un extract Hyper (.hyper) via tableauhyperapi.

    Le fichier produit est directement lisible par Tableau Desktop :
      Connecter → Fichiers supplémentaires → *.hyper

    Chaque dataset QUERIES devient une table dans le schéma 'Extract'.
    Tableau peut créer des relations entre ces tables sur id_salarie.

    Prérequis : pip install tableauhyperapi

    Returns:
        Path du fichier .hyper créé.
    """
    if not HYPER_AVAILABLE:
        raise ImportError(
            "tableauhyperapi n'est pas installé.\n"
            "  pip install tableauhyperapi\n"
            "Puis relancer : python src/export_tableau.py --format hyper"
        )

    import os
    import tableauhyperapi as _hyper_pkg
    _hyper_bin = Path(_hyper_pkg.__file__).parent / "bin" / "hyper"
    if str(_hyper_bin) not in os.environ.get("PATH", ""):
        os.environ["PATH"] = str(_hyper_bin) + os.pathsep + os.environ.get("PATH", "")

    output_dir.mkdir(parents=True, exist_ok=True)
    engine = get_engine()
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    hyper_path = output_dir / f"poc_avantages_sportifs_{ts}.hyper"

    logger.info(f"  💎 Génération Hyper extract → {hyper_path}")

    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            endpoint=hyper.endpoint,
            database=hyper_path,
            create_mode=CreateMode.CREATE_AND_REPLACE,
        ) as connection:

            connection.catalog.create_schema_if_not_exists(SchemaName("Extract"))

            for name, sql in QUERIES.items():
                logger.info(f"  💎 Hyper table '{name}'...")
                df = pd.read_sql(sql.strip(), engine)

                # Convertir les colonnes object en str (Hyper n'accepte pas NaN en text)
                for col in df.select_dtypes(include="object").columns:
                    df[col] = df[col].fillna("").astype(str)

                columns = [
                    TableDefinition.Column(col, _pandas_dtype_to_sql_type(dt))
                    for col, dt in zip(df.columns, df.dtypes)
                ]
                table_def = TableDefinition(
                    table_name=TableName("Extract", name),
                    columns=columns,
                )
                connection.catalog.create_table(table_def)

                with Inserter(connection, table_def) as inserter:
                    for row in df.itertuples(index=False, name=None):
                        inserter.add_row([_to_python(v) for v in row])
                    inserter.execute()

                logger.info(f"     {len(df)} lignes insérées")

    logger.info(f"✅ Hyper extract : {hyper_path.resolve()}")
    return hyper_path


# ════════════════════════════════════════════════════════════════
# Rapport de synthèse console
# ════════════════════════════════════════════════════════════════

def print_kpi_summary() -> None:
    """Affiche les KPI clés pour la démo et la restitution."""
    engine = get_engine()

    df_gold = pd.read_sql("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN eligible_prime    THEN 1 ELSE 0 END) AS nb_prime,
            ROUND(SUM(montant_prime)::NUMERIC, 2)               AS cout_primes,
            SUM(CASE WHEN eligible_jours_be THEN 1 ELSE 0 END) AS nb_be,
            SUM(nb_jours_bienetre)                               AS total_jours,
            MAX(params_version)                                  AS version
        FROM avantages_calcules
    """, engine)

    df_acts = pd.read_sql("""
        SELECT
            COUNT(*) AS total_acts,
            COUNT(DISTINCT employee_id) AS athletes,
            COUNT(DISTINCT sport_type)  AS sports
        FROM strava_activities
    """, engine)

    df_bu = pd.read_sql("""
        SELECT e.bu, COUNT(*) AS n,
               SUM(CASE WHEN a.eligible_prime THEN 1 ELSE 0 END) AS primes,
               ROUND(SUM(a.montant_prime)::NUMERIC, 2) AS cout
        FROM avantages_calcules a JOIN employees e ON e.id = a.employee_id
        GROUP BY e.bu ORDER BY cout DESC NULLS LAST
    """, engine)

    g    = df_gold.iloc[0]
    a    = df_acts.iloc[0]
    line = "═" * 60

    print(f"\n{line}")
    print(f"  📊 KPI FINAUX — POC Avantages Sportifs")
    print(f"  Version paramètres : {g['version']}")
    print(f"{line}")
    print(f"\n  💰 Primes sportives")
    print(f"     Salariés total      : {int(g['total'])}")
    print(f"     Éligibles prime     : {int(g['nb_prime'])} ({int(g['nb_prime'])*100//int(g['total'])} %)")
    print(f"     Coût total          : {float(g['cout_primes']):,.2f} €")
    print(f"\n  🌿 Journées bien-être")
    print(f"     Éligibles jours BE  : {int(g['nb_be'])} ({int(g['nb_be'])*100//int(g['total'])} %)")
    print(f"     Total jours accordés: {int(g['total_jours'])}")
    print(f"\n  🏃 Activités sportives (Monte Carlo 2025)")
    print(f"     Total activités     : {int(a['total_acts'])}")
    print(f"     Athlètes actifs     : {int(a['athletes'])}")
    print(f"     Sports distincts    : {int(a['sports'])}")
    print(f"\n  Ventilation par BU :")
    print(f"  {'BU':<15} {'Salariés':>9} {'Primes':>7} {'€':>12}")
    print(f"  {'-'*45}")
    for _, r in df_bu.iterrows():
        print(f"  {str(r['bu']):<15} {int(r['n']):>9} {int(r['primes']):>7} "
              f"{float(r['cout'] or 0):>12,.2f}")
    print(f"{line}\n")


# ════════════════════════════════════════════════════════════════
# Point d'entrée
# ════════════════════════════════════════════════════════════════

def main(fmt: str = "csv", output_dir: Path = OUTPUT_DIR) -> None:
    """
    Lance l'export Tableau complet.

    Args:
        fmt:        'csv' (v1), 'excel' (v1) ou 'hyper' (v2).
        output_dir: dossier de destination (défaut : reports/tableau/).
    """
    debut = datetime.now()

    if not ping():
        sys.exit(1)

    logger.info("=" * 60)
    logger.info(f"📊 Export Tableau — Round 5 | Format : {fmt} | → {output_dir}")
    logger.info("=" * 60)

    print_kpi_summary()

    if fmt == "hyper":
        path = export_to_hyper(output_dir)
        logger.info(f"\n✅ Hyper extract : {path.resolve()}")
        logger.info("   → Tableau Desktop → Se connecter → Fichiers supplémentaires → *.hyper")
    elif fmt == "excel":
        path = export_to_excel(output_dir)
        logger.info(f"\n✅ Classeur Excel : {path.resolve()}")
        logger.info("   → Tableau Desktop → Se connecter → Microsoft Excel → *.xlsx")
    else:
        results = export_to_csv(output_dir)
        total_rows = sum(results.values())
        logger.info(f"\n✅ {len(results)} fichiers CSV dans {output_dir.resolve()}")
        logger.info(f"   Total : {total_rows} lignes exportées")
        logger.info("   → Tableau Desktop → Se connecter → Fichier texte → *.csv")

    log_run(
        flow_name="export_tableau",
        etape=f"export_{fmt}",
        statut="SUCCESS",
        debut=debut,
        nb_lignes=len(QUERIES),
        metadata={"format": fmt, "output_dir": str(output_dir)},
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export Tableau — Round 5")
    parser.add_argument(
        "--format", choices=["csv", "excel", "hyper"], default="csv",
        help="Format d'export : csv (v1, défaut), excel (v1), hyper (v2 — nécessite tableauhyperapi)",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=OUTPUT_DIR,
        help=f"Dossier de destination (défaut : {OUTPUT_DIR})",
    )
    args = parser.parse_args()
    main(fmt=args.format, output_dir=args.output_dir)

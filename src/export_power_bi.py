"""
Export Power BI — Round 5

Exporte les données PostgreSQL en fichiers CSV prêts à être importés
dans Power BI Desktop. Génère un fichier par page du rapport.

Deux modes :
  ① CSV export  : fichiers dans reports/power_bi/ (import manuel PBI)
  ② Snapshot Excel : un classeur multi-onglets (alternative à ODBC)

Pages générées :
  1. vue_globale.csv          → page "Vue Globale" (KPI synthèse + BU)
  2. primes_sportives.csv     → page "Primes Sportives" (liste éligibles)
  3. journees_bienetre.csv    → page "Journées Bien-être" (activités BE)
  4. activites_sportives.csv  → page "Activités Sportives" (volumes + sports)
  5. anomalies_qualite.csv    → page "Anomalies & Qualité" (contrôles)
  6. config_params.csv        → paramètres config (pour les slicers PBI)

Usage:
    python src/export_power_bi.py
    python src/export_power_bi.py --format excel    # classeur .xlsx
    python src/export_power_bi.py --output-dir reports/power_bi
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
        logging.FileHandler("logs/export_power_bi.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("reports/power_bi")


# ════════════════════════════════════════════════════════════════
# Requêtes SQL — une par page Power BI
# ════════════════════════════════════════════════════════════════

QUERIES: dict[str, str] = {

    # ── Page 1 : Vue Globale ─────────────────────────────────────
    "vue_globale": """
        SELECT
            e.bu                                                          AS BU,
            COUNT(*)                                                      AS Nb_Salaries,
            SUM(CASE WHEN a.eligible_prime    THEN 1 ELSE 0 END)         AS Nb_Eligible_Prime,
            ROUND(SUM(a.montant_prime)::NUMERIC, 2)                      AS Cout_Prime_Euros,
            SUM(CASE WHEN a.eligible_jours_be THEN 1 ELSE 0 END)        AS Nb_Eligible_BE,
            SUM(a.nb_jours_bienetre)                                      AS Total_Jours_BE,
            MAX(a.params_version)                                         AS Params_Version,
            MAX(a.date_calcul)                                            AS Date_Calcul
        FROM avantages_calcules a
        JOIN employees e ON e.id = a.employee_id
        GROUP BY e.bu
        ORDER BY Cout_Prime_Euros DESC
    """,

    # ── Page 2 : Primes Sportives ─────────────────────────────────
    "primes_sportives": """
        SELECT
            e.id                                                          AS ID_Salarie,
            e.nom                                                         AS Nom,
            e.prenom                                                      AS Prenom,
            e.bu                                                          AS BU,
            e.type_contrat                                                AS Type_Contrat,
            e.mode_deplacement                                            AS Mode_Deplacement,
            ROUND(e.distance_bureau_km::NUMERIC, 2)                      AS Distance_Bureau_Km,
            e.salaire_brut                                                AS Salaire_Brut,
            a.eligible_prime                                              AS Eligible_Prime,
            ROUND(a.montant_prime::NUMERIC, 2)                           AS Montant_Prime_Euros,
            a.nb_activites_12m                                            AS Nb_Activites_12m,
            a.eligible_jours_be                                           AS Eligible_Jours_BE,
            a.nb_jours_bienetre                                           AS Nb_Jours_BE,
            a.params_version                                              AS Params_Version
        FROM avantages_calcules a
        JOIN employees e ON e.id = a.employee_id
        ORDER BY a.montant_prime DESC, e.nom
    """,

    # ── Page 3 : Journées Bien-être ───────────────────────────────
    "journees_bienetre": """
        SELECT
            e.id                                                          AS ID_Salarie,
            e.nom                                                         AS Nom,
            e.prenom                                                      AS Prenom,
            e.bu                                                          AS BU,
            e.sport_declare                                               AS Sport_Declare,
            a.nb_activites_12m                                            AS Nb_Activites_12m,
            a.eligible_jours_be                                           AS Eligible_Jours_BE,
            a.nb_jours_bienetre                                           AS Nb_Jours_BE,
            CASE
                WHEN a.nb_activites_12m >= 25 THEN 'Très actif (25+)'
                WHEN a.nb_activites_12m >= 15 THEN 'Actif (15-24) — éligible BE'
                WHEN a.nb_activites_12m >= 5  THEN 'Occasionnel (5-14)'
                ELSE 'Peu actif (0-4)'
            END                                                           AS Tranche_Activite,
            a.params_version                                              AS Params_Version
        FROM avantages_calcules a
        JOIN employees e ON e.id = a.employee_id
        ORDER BY a.nb_activites_12m DESC
    """,

    # ── Page 4 : Activités Sportives ─────────────────────────────
    "activites_sportives": """
        SELECT
            sa.employee_id                                                AS ID_Salarie,
            e.nom                                                         AS Nom,
            e.prenom                                                      AS Prenom,
            e.bu                                                          AS BU,
            sa.sport_type                                                 AS Sport,
            DATE_TRUNC('month', sa.date_debut)::date                     AS Mois,
            EXTRACT(YEAR  FROM sa.date_debut)::int                       AS Annee,
            EXTRACT(MONTH FROM sa.date_debut)::int                       AS Num_Mois,
            TO_CHAR(sa.date_debut, 'Mon YYYY')                           AS Label_Mois,
            COUNT(*)                                                      AS Nb_Activites,
            ROUND(AVG(sa.distance_m / 1000.0)::NUMERIC, 2)              AS Distance_Moy_Km,
            ROUND(AVG(sa.duree_s    / 60.0  )::NUMERIC, 1)              AS Duree_Moy_Min,
            ROUND(SUM(sa.distance_m / 1000.0)::NUMERIC, 2)              AS Distance_Tot_Km,
            sa.source                                                     AS Source
        FROM strava_activities sa
        LEFT JOIN employees e ON e.id = sa.employee_id
        GROUP BY
            sa.employee_id, e.nom, e.prenom, e.bu, sa.sport_type,
            DATE_TRUNC('month', sa.date_debut),
            EXTRACT(YEAR FROM sa.date_debut), EXTRACT(MONTH FROM sa.date_debut),
            TO_CHAR(sa.date_debut, 'Mon YYYY'), sa.source
        ORDER BY Mois, Sport
    """,

    # ── Page 5 : Anomalies & Qualité ────────────────────────────
    "anomalies_qualite": """
        WITH last_run AS (
            SELECT MAX(run_at) AS last_run_at
            FROM data_quality_results
        )
        SELECT
            dqr.run_id                                                    AS Run_ID,
            dqr.run_at                                                    AS Date_Run,
            dqr.suite_name                                                AS Suite,
            dqr.regle                                                     AS Regle,
            dqr.table_cible                                               AS Table_Cible,
            dqr.colonne                                                   AS Colonne,
            dqr.severite                                                  AS Severite,
            dqr.resultat                                                  AS Resultat_OK,
            CASE WHEN dqr.resultat THEN '✅ OK' ELSE
                CASE WHEN dqr.severite = 'BLOQUANT' THEN '🔴 BLOQUANT' ELSE '🟡 WARNING' END
            END                                                           AS Statut_Affiche,
            dqr.detail                                                    AS Detail,
            (dqr.run_at = lr.last_run_at)                                AS Dernier_Run
        FROM data_quality_results dqr
        CROSS JOIN last_run lr
        ORDER BY dqr.run_at DESC, dqr.severite DESC
    """,

    # ── Params config (pour slicers et what-if Power BI) ────────
    "config_params": """
        SELECT
            cle     AS Parametre,
            valeur  AS Valeur,
            description AS Description,
            updated_at AS Mis_A_Jour
        FROM config
        ORDER BY cle
    """,

    # ── Logs pipeline (monitoring) ───────────────────────────────
    "pipeline_logs": """
        SELECT
            id                          AS ID,
            flow_name                   AS Flow,
            etape                       AS Etape,
            statut                      AS Statut,
            debut                       AS Debut,
            fin                         AS Fin,
            duree_ms                    AS Duree_Ms,
            nb_lignes                   AS Nb_Lignes,
            erreur                      AS Erreur
        FROM pipeline_runs
        ORDER BY debut DESC
        LIMIT 200
    """,
}


# ════════════════════════════════════════════════════════════════
# Export
# ════════════════════════════════════════════════════════════════

def export_to_csv(output_dir: Path) -> dict[str, int]:
    """
    Exporte chaque dataset dans un fichier CSV séparé.

    Args:
        output_dir: dossier de destination.

    Returns:
        dict {nom_fichier: nb_lignes} pour chaque export.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    engine = get_engine()
    results = {}

    for name, sql in QUERIES.items():
        logger.info(f"  📊 Export {name}...")
        df = pd.read_sql(sql.strip(), engine)
        path = output_dir / f"{name}.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")  # utf-8-sig = BOM pour Excel
        results[name] = len(df)
        logger.info(f"     {len(df)} lignes → {path}")

    return results


def export_to_excel(output_dir: Path) -> Path:
    """
    Exporte tous les datasets dans un classeur Excel multi-onglets.

    Un onglet par dataset, prêt à être importé dans Power BI
    comme source de données "Excel".

    Args:
        output_dir: dossier de destination.

    Returns:
        Path du fichier .xlsx créé.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    engine = get_engine()
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    path = output_dir / f"poc_avantages_sportifs_{ts}.xlsx"

    page_names = {
        "vue_globale":          "01_Vue_Globale",
        "primes_sportives":     "02_Primes",
        "journees_bienetre":    "03_Journées_BE",
        "activites_sportives":  "04_Activités",
        "anomalies_qualite":    "05_Anomalies",
        "config_params":        "Config",
        "pipeline_logs":        "Logs_Pipeline",
    }

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, sheet_name in page_names.items():
            if name not in QUERIES:
                continue
            logger.info(f"  📋 Onglet '{sheet_name}'...")
            df = pd.read_sql(QUERIES[name].strip(), engine)
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
            logger.info(f"     {len(df)} lignes")

    logger.info(f"✅ Classeur Excel : {path}")
    return path


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
    Lance l'export complet pour Power BI.

    Args:
        fmt:        'csv' ou 'excel'.
        output_dir: dossier de destination (défaut : reports/power_bi/).
    """
    debut = datetime.now()

    if not ping():
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("📊 Export Power BI — Round 5")
    logger.info(f"   Format : {fmt} | Destination : {output_dir}")
    logger.info("=" * 60)

    # KPI console
    print_kpi_summary()

    # Export
    if fmt == "excel":
        path = export_to_excel(output_dir)
        logger.info(f"\n✅ Classeur Excel : {path.resolve()}")
        logger.info(f"   → Ouvrir Power BI Desktop → Obtenir données → Excel")
    else:
        results = export_to_csv(output_dir)
        total_rows = sum(results.values())
        logger.info(f"\n✅ {len(results)} fichiers CSV dans {output_dir.resolve()}")
        logger.info(f"   Total : {total_rows} lignes exportées")
        logger.info(f"   → Ouvrir Power BI Desktop → Obtenir données → Texte/CSV")

    log_run(
        flow_name="export_power_bi",
        etape=f"export_{fmt}",
        statut="SUCCESS",
        debut=debut,
        nb_lignes=len(QUERIES),
        metadata={"format": fmt, "output_dir": str(output_dir)},
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export Power BI — Round 5")
    parser.add_argument("--format", choices=["csv", "excel"], default="csv",
                        help="Format d'export (défaut : csv)")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR,
                        help=f"Dossier de destination (défaut : {OUTPUT_DIR})")
    args = parser.parse_args()
    main(fmt=args.format, output_dir=args.output_dir)

"""
SODA Quality Runner — Round 3

Exécute les contrôles qualité SODA Core sur les tables PostgreSQL.
Remplace la v1 (quality_check.py, 9 règles SQL maison) par des checks
déclaratifs en SodaCL (YAML), plus maintenables et extensibles.

Architecture :
  - soda/checks/employees.yml         → 6 règles Silver
  - soda/checks/strava_activities.yml → 3 règles Bronze
  - soda/checks/avantages_calcules.yml→ 2 règles Gold
  - Résultats sauvegardés dans data_quality_results (même schéma que v1)
  - Rapport HTML dans reports/soda_quality_report.html

SEVERITY_MAP : détermine si un check SODA est BLOQUANT ou WARNING.
  SODA peut produire outcome="fail" pour les deux — c'est notre map
  qui décide si le pipeline est bloqué ou seulement alerté.

Usage:
    python src/soda_runner.py
    python src/soda_runner.py --suite employees
    python src/soda_runner.py --fail-fast
"""
import argparse
import logging
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent))
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from database import get_connection, log_run, ping

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/soda_runner.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

ROOT_DIR    = Path(__file__).resolve().parent.parent
SODA_DIR    = ROOT_DIR / "soda"
CHECKS_DIR  = SODA_DIR / "checks"
REPORTS_DIR = ROOT_DIR / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

# ─── Sévérité par check name ─────────────────────────────────────
# Indépendant de l'outcome SODA (fail/warn/pass) :
# certains checks "failed rows" produisent outcome="fail" même pour
# des anomalies WARNING. Cette map est la référence de sévérité.
SEVERITY_MAP: dict[str, str] = {
    # Silver — employees
    "mode_deplacement_enum":       "BLOQUANT",
    "id_sans_decimal":             "BLOQUANT",
    "eligible_prime_coherence":    "BLOQUANT",
    "salaire_plausible":           "WARNING",
    "anomalie_declaration_marche": "WARNING",
    "anomalie_declaration_velo":   "WARNING",
    # Bronze — strava_activities
    "distance_sport_positive":     "BLOQUANT",
    "employee_id_fk":              "BLOQUANT",
    "dates_strava_fenetre_2025":   "WARNING",
    # Gold — avantages_calcules
    "gold_non_vide":               "BLOQUANT",
    "prime_coherence_gold":        "BLOQUANT",
}

CHECK_FILES_BY_SUITE: dict[str, list[str]] = {
    "employees":  ["employees.yml"],
    "strava":     ["strava_activities.yml"],
    "gold":       ["avantages_calcules.yml"],
    "all":        ["employees.yml", "strava_activities.yml", "avantages_calcules.yml"],
}


# ════════════════════════════════════════════════════════════════
# 1. Configuration SODA (injectée dynamiquement)
# ════════════════════════════════════════════════════════════════

def _build_config_yaml() -> str:
    """Génère la configuration SODA avec les credentials de config.py."""
    return f"""
data_sources:
  poc_sport:
    type: postgres
    host: {DB_HOST}
    port: {DB_PORT}
    username: {DB_USER}
    password: {DB_PASSWORD}
    database: {DB_NAME}
"""


# ════════════════════════════════════════════════════════════════
# 2. Exécution du scan
# ════════════════════════════════════════════════════════════════

def run_soda_scan(suite: str = "all") -> dict:
    """
    Exécute les checks SODA pour la suite demandée.

    Args:
        suite: 'all', 'employees', 'strava', ou 'gold'.

    Returns:
        dict brut retourné par scan.get_scan_results().
    """
    try:
        from soda.scan import Scan
    except ImportError:
        logger.error(
            "soda-core-postgres non installé.\n"
            "  Installer : pip install soda-core-postgres"
        )
        sys.exit(1)

    files = CHECK_FILES_BY_SUITE.get(suite, CHECK_FILES_BY_SUITE["all"])
    check_paths = [CHECKS_DIR / f for f in files]

    scan = Scan()
    scan.set_data_source_name("poc_sport")
    scan.add_configuration_yaml_str(_build_config_yaml())

    for path in check_paths:
        if not path.exists():
            logger.warning(f"  Fichier check absent : {path}")
            continue
        scan.add_sodacl_yaml_file(str(path))
        logger.info(f"  Chargé : {path.name}")

    logger.info(f"\n  Exécution SODA — suite : {suite.upper()}")
    exit_code = scan.execute()

    logs_text = scan.get_logs_text()
    if exit_code != 0:
        logger.warning(f"  SODA exit_code={exit_code}\n{logs_text}")
    else:
        logger.info(f"  SODA scan terminé (exit_code={exit_code})")

    return scan.get_scan_results()


# ════════════════════════════════════════════════════════════════
# 3. Parsing des résultats
# ════════════════════════════════════════════════════════════════

def parse_soda_results(raw: dict) -> list[dict]:
    """
    Convertit les résultats bruts SODA en liste de dicts normalisés,
    compatibles avec le schéma data_quality_results.

    Args:
        raw: dict retourné par scan.get_scan_results().

    Returns:
        Liste de dicts {regle, table_cible, colonne, severite,
                        nb_total, nb_echecs, resultat, detail}.
    """
    parsed: list[dict] = []

    for check in raw.get("checks", []):
        name    = check.get("name", "unnamed")
        outcome = check.get("outcome", "unknown")  # pass | warn | fail | unknown
        table   = check.get("table", "unknown")
        column  = check.get("column") or "—"
        diag    = check.get("diagnostics", {})

        # Nombre d'échecs : plusieurs clés possibles selon le type de check
        nb_echecs = 0
        if outcome != "pass":
            nb_echecs = int(
                diag.get("failedRowCount")
                or diag.get("value")
                or 0
            )

        nb_total = int(diag.get("rowCount") or -1)

        severite = SEVERITY_MAP.get(name, "WARNING")
        resultat = outcome == "pass"

        detail = _build_detail(name, outcome, severite, nb_echecs, nb_total, diag)

        parsed.append({
            "regle":       name,
            "table_cible": table,
            "colonne":     column,
            "severite":    severite,
            "nb_total":    nb_total,
            "nb_echecs":   nb_echecs,
            "resultat":    resultat,
            "detail":      detail,
        })

    return parsed


def _build_detail(
    name: str,
    outcome: str,
    severite: str,
    nb_echecs: int,
    nb_total: int,
    diag: dict,
) -> str:
    if outcome == "pass":
        total_str = f" ({nb_total} lignes vérifiées)" if nb_total > 0 else ""
        return f"Tous les contrôles passés{total_str}."

    label = f"[{severite}]"
    total_str = f"/{nb_total}" if nb_total > 0 else ""

    if nb_echecs > 0:
        return f"{label} {nb_echecs}{total_str} ligne(s) en échec sur la règle « {name} »."

    value = diag.get("value")
    if value is not None:
        return f"{label} Valeur mesurée : {value} (règle « {name} »)."

    return f"{label} Échec détecté — règle « {name} »."


# ════════════════════════════════════════════════════════════════
# 4. Persistance
# ════════════════════════════════════════════════════════════════

def save_results(results: list[dict], run_id: str) -> None:
    """Insère les résultats dans data_quality_results (même table que v1)."""
    rows = [
        (
            run_id,
            "poc_avantages_sportifs_soda_v2",
            r["regle"],
            r["table_cible"],
            r["colonne"],
            r["resultat"],
            r["severite"],
            r["detail"],
        )
        for r in results
    ]
    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO data_quality_results
                    (run_id, suite_name, regle, table_cible, colonne,
                     resultat, severite, detail)
                VALUES %s
                """,
                rows,
            )
    logger.info(f"💾 {len(rows)} résultats sauvegardés (run_id={run_id[:8]}…)")


# ════════════════════════════════════════════════════════════════
# 5. Rapport HTML
# ════════════════════════════════════════════════════════════════

def generate_html_report(results: list[dict], run_id: str) -> Path:
    """Génère reports/soda_quality_report.html."""
    ts           = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    nb_ok        = sum(1 for r in results if r["resultat"])
    nb_total     = len(results)
    nb_bloquant  = sum(1 for r in results if not r["resultat"] and r["severite"] == "BLOQUANT")
    nb_warn      = sum(1 for r in results if not r["resultat"] and r["severite"] == "WARNING")

    if nb_ok == nb_total:
        status_txt = "✅ TOUT PASSÉ"
        bg_status  = "#d1fae5"
    elif nb_bloquant:
        status_txt = "🔴 BLOQUANT"
        bg_status  = "#fee2e2"
    else:
        status_txt = "🟡 AVERTISSEMENTS"
        bg_status  = "#fef9c3"

    rows_html = ""
    for r in results:
        icon = "✅" if r["resultat"] else ("🔴" if r["severite"] == "BLOQUANT" else "🟡")
        bg   = "#f0fdf4" if r["resultat"] else (
            "#fee2e2" if r["severite"] == "BLOQUANT" else "#fef9c3"
        )
        badge_color = "#dc2626" if r["severite"] == "BLOQUANT" else "#d97706"
        badge = (
            f'<span style="background:{badge_color};color:white;'
            f'padding:2px 8px;border-radius:4px;font-size:11px">'
            f'{r["severite"]}</span>'
        )
        total_str = str(r["nb_total"]) if r["nb_total"] >= 0 else "?"
        rows_html += f"""
        <tr style="background:{bg}">
          <td style="padding:10px">{icon}</td>
          <td style="padding:10px;font-weight:bold">{r['regle']}</td>
          <td style="padding:10px;font-family:monospace">{r['table_cible']}</td>
          <td style="padding:10px;font-family:monospace">{r['colonne']}</td>
          <td style="padding:10px">{badge}</td>
          <td style="padding:10px;text-align:right">{r['nb_echecs']} / {total_str}</td>
          <td style="padding:10px;font-size:12px;color:#555">{r['detail']}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <title>Rapport SODA — POC Avantages Sportifs</title>
  <style>
    body  {{ font-family: Arial, sans-serif; margin: 40px; color: #1f2937; }}
    h1   {{ color: #1e3a5c; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
    th   {{ background: #1e3a5c; color: white; padding: 12px; text-align: left; }}
    td   {{ border-bottom: 1px solid #e5e7eb; }}
    .summary {{ display: flex; gap: 20px; margin: 20px 0; }}
    .card    {{ background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px;
                padding: 16px; text-align: center; min-width: 130px; }}
    .card h2 {{ margin: 4px 0; font-size: 28px; }}
    .card p  {{ margin: 0; font-size: 13px; color: #64748b; }}
    .badge-soda {{ background:#0d9488;color:white;padding:3px 10px;
                   border-radius:12px;font-size:12px;font-weight:600; }}
  </style>
</head>
<body>
  <h1>Rapport Qualité SODA — POC Avantages Sportifs</h1>
  <p>
    <span class="badge-soda">SODA Core v2</span>
    &nbsp; Run ID : <code>{run_id}</code> · Généré le : {ts}
  </p>

  <div style="background:{bg_status};border-radius:8px;padding:16px;
              font-size:20px;font-weight:bold;margin-bottom:20px">
    Statut global : {status_txt}
  </div>

  <div class="summary">
    <div class="card"><h2>{nb_ok}</h2><p>Règles OK</p></div>
    <div class="card"><h2>{nb_total - nb_ok}</h2><p>Règles KO</p></div>
    <div class="card"><h2>{nb_bloquant}</h2><p>Bloquants</p></div>
    <div class="card"><h2>{nb_warn}</h2><p>Warnings</p></div>
    <div class="card"><h2>{nb_total}</h2><p>Total règles</p></div>
  </div>

  <table>
    <thead>
      <tr>
        <th>Statut</th><th>Règle</th><th>Table</th><th>Colonne</th>
        <th>Sévérité</th><th>Échecs</th><th>Détail</th>
      </tr>
    </thead>
    <tbody>{rows_html}</tbody>
  </table>

  <hr style="margin-top:40px">
  <p style="font-size:12px;color:#94a3b8">
    POC Avantages Sportifs · Round 3 · SODA Core · {nb_ok}/{nb_total} règles passées ·
    Quality Check v1 : <code>src/quality_check.py</code> (référence)
  </p>
</body>
</html>"""

    report_path = REPORTS_DIR / "soda_quality_report.html"
    report_path.write_text(html, encoding="utf-8")
    logger.info(f"📄 Rapport HTML : {report_path}")
    return report_path


# ════════════════════════════════════════════════════════════════
# 6. Point d'entrée
# ════════════════════════════════════════════════════════════════

def run_suite(suite: str = "all", fail_fast: bool = False) -> tuple[list[dict], bool]:
    """
    Orchestre le scan SODA et retourne les résultats parsés.

    Returns:
        (results, pipeline_ok)
        pipeline_ok = False si au moins un check BLOQUANT a échoué.
    """
    raw     = run_soda_scan(suite)
    results = parse_soda_results(raw)

    logger.info(f"\n{'─'*65}")
    logger.info(f"  SODA Quality — {len(results)} règles [{suite.upper()}]")
    logger.info(f"{'─'*65}")

    pipeline_ok = True
    for r in results:
        icon = "✅" if r["resultat"] else ("🔴" if r["severite"] == "BLOQUANT" else "🟡")
        logger.info(
            f"  {icon} [{r['severite']:8s}] {r['regle']:<35s} "
            f"échecs={r['nb_echecs']}"
        )
        if not r["resultat"] and r["severite"] == "BLOQUANT":
            pipeline_ok = False
            if fail_fast:
                logger.error(f"  🔴 FAIL FAST — arrêt sur : {r['regle']}")
                break

    nb_ok       = sum(1 for r in results if r["resultat"])
    nb_bloquant = sum(1 for r in results if not r["resultat"] and r["severite"] == "BLOQUANT")
    nb_warn     = sum(1 for r in results if not r["resultat"] and r["severite"] == "WARNING")
    logger.info(
        f"\n  RÉSUMÉ SODA : {nb_ok}/{len(results)} OK · "
        f"{nb_bloquant} bloquant(s) · {nb_warn} warning(s)"
    )

    return results, pipeline_ok


def main(suite: str = "all", fail_fast: bool = False) -> bool:
    """
    Point d'entrée principal.

    Returns:
        True si pipeline valide (aucun BLOQUANT échoué).
    """
    debut = datetime.now()

    if not ping():
        sys.exit(1)

    run_id  = str(uuid.uuid4())
    results, pipeline_ok = run_suite(suite=suite, fail_fast=fail_fast)

    save_results(results, run_id)
    report_path = generate_html_report(results, run_id)

    nb_ok = sum(1 for r in results if r["resultat"])
    log_run(
        flow_name="soda_quality",
        etape=f"suite={suite}",
        statut="SUCCESS" if pipeline_ok else "FAILED",
        debut=debut,
        nb_lignes=len(results),
        metadata={
            "run_id":       run_id,
            "engine":       "soda-core-postgres",
            "nb_ok":        nb_ok,
            "nb_bloquants": sum(1 for r in results if not r["resultat"] and r["severite"] == "BLOQUANT"),
            "nb_warnings":  sum(1 for r in results if not r["resultat"] and r["severite"] == "WARNING"),
            "report":       str(report_path),
        },
    )

    print(f"\n  📄 Rapport SODA : {report_path.resolve()}")
    if not pipeline_ok:
        print("  🔴 Pipeline invalidé : des règles BLOQUANTES ont échoué.")
    else:
        print("  ✅ Tous les contrôles BLOQUANTS sont passés.")

    return pipeline_ok


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SODA Quality Runner — Round 3")
    parser.add_argument(
        "--suite",
        choices=["all", "employees", "strava", "gold"],
        default="all",
        help="Suite de checks à exécuter (défaut : all)",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stoppe dès le premier BLOQUANT échoué",
    )
    args = parser.parse_args()
    ok = main(suite=args.suite, fail_fast=args.fail_fast)
    sys.exit(0 if ok else 1)

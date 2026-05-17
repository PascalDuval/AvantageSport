"""
Contrôles Qualité — Round 3

Implémente 8 règles de validation sur les données du pipeline.
Résultats stockés dans `data_quality_results` (PostgreSQL).
Rapport HTML généré dans `reports/quality_report.html`.

Architecture :
  - Chaque règle est une fonction SQL qui retourne (nb_total, nb_echecs)
  - Sévérité BLOQUANT : le pipeline doit s'arrêter si nb_echecs > 0
  - Sévérité WARNING   : signalé mais n'arrête pas le pipeline
  - Le rapport HTML résume toutes les règles de façon lisible

Règles :
  ① BLOQUANT  employees     : distance ≤ seuil pour marche_running
  ② BLOQUANT  employees     : distance ≤ seuil pour velo_trottinette
  ③ BLOQUANT  employees     : mode_deplacement dans l'enum attendu
  ④ BLOQUANT  employees     : aucun ID avec décimale ('.0')
  ⑤ BLOQUANT  strava_activ. : distance_m > 0 quand non NULL
  ⑥ BLOQUANT  strava_activ. : employee_id existe dans employees
  ⑦ WARNING   employees     : salaire_brut dans une plage plausible
  ⑧ WARNING   strava_activ. : date_debut dans la fenêtre 2025

Usage:
    python src/quality_check.py
    python src/quality_check.py --fail-fast     # stoppe au 1er bloquant
    python src/quality_check.py --suite silver  # seulement règles employees
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
from database import get_connection, get_config, log_run, ping

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/quality_check.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)

SEVERITE_BLOQUANT = "BLOQUANT"
SEVERITE_WARNING  = "WARNING"


# ════════════════════════════════════════════════════════════════
# 1. Moteur de règles
# ════════════════════════════════════════════════════════════════

class QualityResult:
    """Résultat d'une règle de qualité."""

    def __init__(
        self,
        regle: str,
        table_cible: str,
        colonne: str,
        severite: str,
        nb_total: int,
        nb_echecs: int,
        detail: str,
    ):
        self.regle       = regle
        self.table_cible = table_cible
        self.colonne     = colonne
        self.severite    = severite
        self.nb_total    = nb_total
        self.nb_echecs   = nb_echecs
        self.resultat    = nb_echecs == 0      # True = règle passée
        self.detail      = detail

    @property
    def status_icon(self) -> str:
        if self.resultat:
            return "✅"
        return "🔴" if self.severite == SEVERITE_BLOQUANT else "🟡"

    def __repr__(self):
        return (
            f"{self.status_icon} [{self.severite}] {self.regle} "
            f"| {self.nb_echecs}/{self.nb_total} échecs"
        )


def run_sql_check(
    sql_total: str,
    sql_echecs: str,
    regle: str,
    table_cible: str,
    colonne: str,
    severite: str,
    detail: str,
) -> QualityResult:
    """
    Exécute une vérification qualité via deux requêtes SQL.

    Args:
        sql_total:        requête retournant le nombre total de lignes à vérifier.
        sql_echecs:       requête retournant les lignes en échec.
        regle:            nom de la règle (affiché dans le rapport).
        table_cible:      nom de la table vérifiée.
        colonne:          colonne concernée.
        severite:         BLOQUANT ou WARNING.
        detail:           message de détail (peut inclure {nb_echecs} et {nb_total}).

    Returns:
        QualityResult avec les métriques.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_total)
            nb_total = cur.fetchone()[0]

            cur.execute(sql_echecs)
            nb_echecs = cur.fetchone()[0]

    detail = detail.format(nb_echecs=nb_echecs, nb_total=nb_total)
    return QualityResult(regle, table_cible, colonne, severite, nb_total, nb_echecs, detail)


# ════════════════════════════════════════════════════════════════
# 2. Les 8 règles
# ════════════════════════════════════════════════════════════════

def check_distance_marche() -> QualityResult:
    """
    WARNING — Règle ①
    Détecte les déclarations suspectes : salarié déclarant marche/running
    mais domicilié à plus de seuil_marche_km du bureau.
    Ces salariés sont déjà exclus du calcul de prime (eligible_prime = False),
    mais l'anomalie doit être remontée sans bloquer le pipeline.
    Cause probable : erreur de déclaration ou changement de situation non mis à jour.
    """
    seuil = float(get_config("seuil_marche_km", "15.0"))
    return run_sql_check(
        sql_total="""
            SELECT COUNT(*) FROM employees
            WHERE mode_deplacement = 'marche_running'
              AND distance_bureau_km IS NOT NULL
        """,
        sql_echecs=f"""
            SELECT COUNT(*) FROM employees
            WHERE mode_deplacement = 'marche_running'
              AND distance_bureau_km IS NOT NULL
              AND distance_bureau_km > {seuil}
        """,
        regle="anomalie_declaration_marche",
        table_cible="employees",
        colonne="distance_bureau_km",
        severite=SEVERITE_WARNING,
        detail=f"Seuil marche/running = {seuil} km. {{nb_echecs}} salarié(s) déclarent venir à pied/en courant mais habitent à plus de {seuil} km (erreur de déclaration probable — exclus de la prime, pipeline non bloqué).",
    )


def check_distance_velo() -> QualityResult:
    """
    WARNING — Règle ②
    Détecte les déclarations suspectes : salarié déclarant vélo/trottinette
    mais domicilié à plus de seuil_velo_km du bureau.
    Ces salariés sont déjà exclus du calcul de prime (eligible_prime = False),
    mais l'anomalie doit être remontée sans bloquer le pipeline.
    """
    seuil = float(get_config("seuil_velo_km", "25.0"))
    return run_sql_check(
        sql_total="""
            SELECT COUNT(*) FROM employees
            WHERE mode_deplacement = 'velo_trottinette'
              AND distance_bureau_km IS NOT NULL
        """,
        sql_echecs=f"""
            SELECT COUNT(*) FROM employees
            WHERE mode_deplacement = 'velo_trottinette'
              AND distance_bureau_km IS NOT NULL
              AND distance_bureau_km > {seuil}
        """,
        regle="anomalie_declaration_velo",
        table_cible="employees",
        colonne="distance_bureau_km",
        severite=SEVERITE_WARNING,
        detail=f"Seuil vélo/trottinette = {seuil} km. {{nb_echecs}} salarié(s) déclarent venir à vélo/trottinette mais habitent à plus de {seuil} km (erreur de déclaration probable — exclus de la prime, pipeline non bloqué).",
    )


def check_eligible_prime_coherence() -> QualityResult:
    """
    BLOQUANT — Règle ①bis
    Incohérence ETL : un salarié ne peut pas avoir eligible_prime = TRUE
    si sa distance dépasse le seuil de son mode. Si ce cas survient,
    c'est un bug dans compute_eligible_prime ou une mise à jour manuelle incorrecte.
    """
    seuil_marche = float(get_config("seuil_marche_km", "15.0"))
    seuil_velo   = float(get_config("seuil_velo_km", "25.0"))
    return run_sql_check(
        sql_total="SELECT COUNT(*) FROM employees WHERE eligible_prime = TRUE",
        sql_echecs=f"""
            SELECT COUNT(*) FROM employees
            WHERE eligible_prime = TRUE
              AND (
                (mode_deplacement = 'marche_running'   AND distance_bureau_km > {seuil_marche})
             OR (mode_deplacement = 'velo_trottinette' AND distance_bureau_km > {seuil_velo})
             OR mode_deplacement IN ('transports_commun', 'vehicule')
              )
        """,
        regle="eligible_prime_coherence_etl",
        table_cible="employees",
        colonne="eligible_prime",
        severite=SEVERITE_BLOQUANT,
        detail=f"{{nb_echecs}} salarié(s) marqués eligible_prime=True alors que les règles métier (marche ≤ {seuil_marche} km, vélo ≤ {seuil_velo} km, TC/véhicule jamais éligibles) l'interdisent. Bug ETL détecté.",
    )


def check_mode_enum() -> QualityResult:
    """
    BLOQUANT — Règle ③
    mode_deplacement doit être dans les 4 valeurs de l'enum normalisé.
    """
    return run_sql_check(
        sql_total="SELECT COUNT(*) FROM employees WHERE mode_deplacement IS NOT NULL",
        sql_echecs="""
            SELECT COUNT(*) FROM employees
            WHERE mode_deplacement IS NOT NULL
              AND mode_deplacement NOT IN (
                  'marche_running', 'velo_trottinette',
                  'transports_commun', 'vehicule'
              )
        """,
        regle="mode_deplacement_enum",
        table_cible="employees",
        colonne="mode_deplacement",
        severite=SEVERITE_BLOQUANT,
        detail="{nb_echecs} salariés avec un mode de déplacement hors enum (valeur brute non normalisée).",
    )


def check_ids_sans_decimal() -> QualityResult:
    """
    BLOQUANT — Règle ④
    Aucun ID salarié ne doit contenir un point (ex : '59019.0' → non normalisé).
    """
    return run_sql_check(
        sql_total="SELECT COUNT(*) FROM employees",
        sql_echecs="SELECT COUNT(*) FROM employees WHERE id LIKE '%.%'",
        regle="id_sans_decimal",
        table_cible="employees",
        colonne="id",
        severite=SEVERITE_BLOQUANT,
        detail="{nb_echecs} IDs contiennent encore un point décimal (normalisation IDs incomplète).",
    )


def check_distance_sport_positive() -> QualityResult:
    """
    BLOQUANT — Règle ⑤
    distance_m > 0 pour toutes les activités qui ont une distance (non NULL).
    """
    return run_sql_check(
        sql_total="SELECT COUNT(*) FROM strava_activities WHERE distance_m IS NOT NULL",
        sql_echecs="SELECT COUNT(*) FROM strava_activities WHERE distance_m IS NOT NULL AND distance_m <= 0",
        regle="distance_sport_positive",
        table_cible="strava_activities",
        colonne="distance_m",
        severite=SEVERITE_BLOQUANT,
        detail="{nb_echecs} activités avec distance_m ≤ 0 (données incohérentes).",
    )


def check_employee_id_fk() -> QualityResult:
    """
    BLOQUANT — Règle ⑥
    Chaque employee_id dans strava_activities doit exister dans employees.
    """
    return run_sql_check(
        sql_total="SELECT COUNT(DISTINCT employee_id) FROM strava_activities",
        sql_echecs="""
            SELECT COUNT(DISTINCT sa.employee_id)
            FROM strava_activities sa
            LEFT JOIN employees e ON e.id = sa.employee_id
            WHERE e.id IS NULL
        """,
        regle="employee_id_fk",
        table_cible="strava_activities",
        colonne="employee_id",
        severite=SEVERITE_BLOQUANT,
        detail="{nb_echecs} employee_id distincts dans strava_activities absents de la table employees.",
    )


def check_salaire_plausible() -> QualityResult:
    """
    WARNING — Règle ⑦
    salaire_brut doit être > 0 et < 500 000.
    """
    return run_sql_check(
        sql_total="SELECT COUNT(*) FROM employees WHERE salaire_brut IS NOT NULL",
        sql_echecs="SELECT COUNT(*) FROM employees WHERE salaire_brut <= 0 OR salaire_brut > 500000",
        regle="salaire_plausible",
        table_cible="employees",
        colonne="salaire_brut",
        severite=SEVERITE_WARNING,
        detail="{nb_echecs} salariés avec salaire ≤ 0 ou > 500 000 € (valeurs aberrantes).",
    )


def check_dates_strava_2025() -> QualityResult:
    """
    WARNING — Règle ⑧
    Les dates d'activité doivent être dans la fenêtre 2025-01-01 → 2025-12-31.
    (cohérent avec la simulation Monte Carlo Round 1)
    """
    return run_sql_check(
        sql_total="SELECT COUNT(*) FROM strava_activities",
        sql_echecs="""
            SELECT COUNT(*) FROM strava_activities
            WHERE date_debut < '2025-01-01'
               OR date_debut > '2025-12-31 23:59:59'
        """,
        regle="dates_strava_fenetre_2025",
        table_cible="strava_activities",
        colonne="date_debut",
        severite=SEVERITE_WARNING,
        detail=(
            "{nb_echecs}/{nb_total} activités hors de la fenêtre 2025-01-01 → 2025-12-31 "
            "(attendu : données Monte Carlo confinées à 2025)."
        ),
    )


# Suite complète
RULES_SILVER = [
    check_distance_marche,           # WARNING  — anomalie déclaration marche > seuil
    check_distance_velo,             # WARNING  — anomalie déclaration vélo > seuil
    check_eligible_prime_coherence,  # BLOQUANT — incohérence ETL eligible_prime
    check_mode_enum,
    check_ids_sans_decimal,
    check_salaire_plausible,
]

RULES_BRONZE = [
    check_distance_sport_positive,
    check_employee_id_fk,
    check_dates_strava_2025,
]

ALL_RULES = RULES_SILVER + RULES_BRONZE


# ════════════════════════════════════════════════════════════════
# 3. Sauvegarde en base
# ════════════════════════════════════════════════════════════════

def save_results(results: list[QualityResult], run_id: str) -> None:
    """
    Persiste les résultats dans la table `data_quality_results`.

    Args:
        results: liste de QualityResult.
        run_id:  UUID du run (relie les résultats entre eux).
    """
    rows = [
        (
            run_id,
            "poc_avantages_sportifs",   # suite_name
            r.regle,
            r.table_cible,
            r.colonne,
            r.resultat,
            r.severite,
            r.detail,
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
    logger.info(f"💾 {len(rows)} résultats sauvegardés dans data_quality_results")


# ════════════════════════════════════════════════════════════════
# 4. Rapport HTML
# ════════════════════════════════════════════════════════════════

def generate_html_report(results: list[QualityResult], run_id: str) -> Path:
    """
    Génère un rapport HTML lisible dans reports/quality_report.html.

    Args:
        results: liste de QualityResult.
        run_id:  UUID du run (affiché dans le rapport).

    Returns:
        Path vers le fichier HTML généré.
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    nb_ok      = sum(1 for r in results if r.resultat)
    nb_total   = len(results)
    nb_bloquant_fail = sum(1 for r in results if not r.resultat and r.severite == SEVERITE_BLOQUANT)

    status_global = "✅ TOUT PASSÉ" if nb_ok == nb_total else (
        "🔴 BLOQUANT" if nb_bloquant_fail > 0 else "🟡 AVERTISSEMENTS"
    )
    bg_status = "#d1fae5" if nb_ok == nb_total else (
        "#fee2e2" if nb_bloquant_fail > 0 else "#fef9c3"
    )

    rows_html = ""
    for r in results:
        icon  = r.status_icon
        bg    = "#f0fdf4" if r.resultat else ("#fee2e2" if r.severite == SEVERITE_BLOQUANT else "#fef9c3")
        badge = (
            '<span style="background:#dc2626;color:white;padding:2px 8px;border-radius:4px;font-size:11px">BLOQUANT</span>'
            if r.severite == SEVERITE_BLOQUANT else
            '<span style="background:#d97706;color:white;padding:2px 8px;border-radius:4px;font-size:11px">WARNING</span>'
        )
        rows_html += f"""
        <tr style="background:{bg}">
          <td style="padding:10px">{icon}</td>
          <td style="padding:10px;font-weight:bold">{r.regle}</td>
          <td style="padding:10px;font-family:monospace">{r.table_cible}</td>
          <td style="padding:10px;font-family:monospace">{r.colonne}</td>
          <td style="padding:10px">{badge}</td>
          <td style="padding:10px;text-align:right">{r.nb_echecs} / {r.nb_total}</td>
          <td style="padding:10px;font-size:12px;color:#555">{r.detail}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <title>Rapport Qualité — POC Avantages Sportifs</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 40px; color: #1f2937; }}
    h1   {{ color: #1e3a5c; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
    th   {{ background: #1e3a5c; color: white; padding: 12px; text-align: left; }}
    td   {{ border-bottom: 1px solid #e5e7eb; }}
    .summary {{ display: flex; gap: 20px; margin: 20px 0; }}
    .card    {{ background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px;
                padding: 16px; text-align: center; min-width: 130px; }}
    .card h2 {{ margin: 4px 0; font-size: 28px; }}
    .card p  {{ margin: 0; font-size: 13px; color: #64748b; }}
  </style>
</head>
<body>
  <h1>Rapport Qualité — POC Avantages Sportifs</h1>
  <p>Run ID : <code>{run_id}</code> · Généré le : {ts}</p>

  <div style="background:{bg_status};border-radius:8px;padding:16px;font-size:20px;font-weight:bold;margin-bottom:20px">
    Statut global : {status_global}
  </div>

  <div class="summary">
    <div class="card">
      <h2>{nb_ok}</h2><p>Règles passées</p>
    </div>
    <div class="card">
      <h2>{nb_total - nb_ok}</h2><p>Règles échouées</p>
    </div>
    <div class="card">
      <h2>{nb_bloquant_fail}</h2><p>Bloquants</p>
    </div>
    <div class="card">
      <h2>{nb_total}</h2><p>Règles totales</p>
    </div>
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
    POC Avantages Sportifs · Round 3 · Quality Check · {nb_ok}/{nb_total} règles passées
  </p>
</body>
</html>"""

    report_path = REPORTS_DIR / "quality_report.html"
    report_path.write_text(html, encoding="utf-8")
    logger.info(f"📄 Rapport HTML généré : {report_path}")
    return report_path


# ════════════════════════════════════════════════════════════════
# 5. Point d'entrée
# ════════════════════════════════════════════════════════════════

def run_suite(
    suite: str = "all",
    fail_fast: bool = False,
) -> tuple[list[QualityResult], bool]:
    """
    Exécute la suite de règles qualité sélectionnée.

    Args:
        suite:     'all', 'silver' (employees) ou 'bronze' (strava).
        fail_fast: si True, stoppe dès le premier BLOQUANT échoué.

    Returns:
        (results, pipeline_ok) — pipeline_ok = False si au moins 1 BLOQUANT échoue.
    """
    if suite == "silver":
        rules = RULES_SILVER
    elif suite == "bronze":
        rules = RULES_BRONZE
    else:
        rules = ALL_RULES

    logger.info(f"\n{'─'*65}")
    logger.info(f"  ✅ Quality Check — {len(rules)} règles [{suite.upper()}]")
    logger.info(f"{'─'*65}")

    results = []
    pipeline_ok = True

    for rule_fn in rules:
        result = rule_fn()
        results.append(result)
        logger.info(str(result))

        if not result.resultat and result.severite == SEVERITE_BLOQUANT:
            pipeline_ok = False
            if fail_fast:
                logger.error(f"🔴 FAIL FAST activé — arrêt sur : {result.regle}")
                break

    nb_ok = sum(1 for r in results if r.resultat)
    nb_bloquant = sum(1 for r in results if not r.resultat and r.severite == SEVERITE_BLOQUANT)
    nb_warn     = sum(1 for r in results if not r.resultat and r.severite == SEVERITE_WARNING)

    logger.info(f"\n  RÉSUMÉ : {nb_ok}/{len(results)} règles OK · {nb_bloquant} bloquant(s) · {nb_warn} warning(s)")

    return results, pipeline_ok


def main(suite: str = "all", fail_fast: bool = False) -> bool:
    """
    Orchestre le contrôle qualité complet.

    Returns:
        bool: True si pipeline valide (aucun BLOQUANT échoué).
    """
    debut = datetime.now()

    if not ping():
        sys.exit(1)

    run_id = str(uuid.uuid4())
    results, pipeline_ok = run_suite(suite=suite, fail_fast=fail_fast)

    save_results(results, run_id)
    report_path = generate_html_report(results, run_id)

    log_run(
        flow_name="quality_check",
        etape=f"suite={suite}",
        statut="SUCCESS" if pipeline_ok else "FAILED",
        debut=debut,
        nb_lignes=len(results),
        metadata={
            "run_id":       run_id,
            "nb_ok":        sum(1 for r in results if r.resultat),
            "nb_bloquants": sum(1 for r in results if not r.resultat and r.severite == SEVERITE_BLOQUANT),
            "nb_warnings":  sum(1 for r in results if not r.resultat and r.severite == SEVERITE_WARNING),
            "report":       str(report_path),
        },
    )

    print(f"\n  📄 Rapport HTML : {report_path.resolve()}")
    if not pipeline_ok:
        print("  🔴 Pipeline invalidé : des règles BLOQUANTES ont échoué.")
    else:
        print("  ✅ Tous les contrôles BLOQUANTS sont passés.")

    return pipeline_ok


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Contrôles qualité — Round 3")
    parser.add_argument("--suite", choices=["all", "silver", "bronze"], default="all",
                        help="Suite de règles à exécuter (défaut : all)")
    parser.add_argument("--fail-fast", action="store_true",
                        help="Stoppe dès le premier BLOQUANT échoué")
    args = parser.parse_args()

    ok = main(suite=args.suite, fail_fast=args.fail_fast)
    sys.exit(0 if ok else 1)

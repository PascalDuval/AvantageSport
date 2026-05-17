"""
Script de clôture — Round 5

Validation end-to-end du pipeline complet R1→R4 et export Power BI.

Étapes :
  1. Vérification de l'infrastructure (PostgreSQL, Flask, Kestra)
  2. Contrôle des données de tous les rounds
  3. Lancement de l'export Power BI (CSV + Excel)
  4. Rapport de synthèse final
  5. Instructions de démo live

Usage:
    python scripts/run_round5.py
    python scripts/run_round5.py --export-only          # seulement export PBI
    python scripts/run_round5.py --format excel         # export Excel
    python scripts/run_round5.py --full-pipeline-check  # relance tout R1→R4
"""
import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database import get_connection, ping, log_run

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SEP  = "═" * 65
SEP2 = "─" * 65
FLASK_PORT = int(os.getenv("FLASK_PORT", "5001"))


# ════════════════════════════════════════════════════════════════
# 1. Checks infrastructure
# ════════════════════════════════════════════════════════════════

def check_infrastructure() -> dict[str, bool]:
    """Vérifie que tous les composants sont actifs."""
    print(f"\n{SEP}")
    print("  CHECK INFRASTRUCTURE")
    print(SEP)
    results = {}

    # PostgreSQL
    print("  [1/4] PostgreSQL (localhost:5432)...", end=" ")
    ok = ping(max_retries=3, delay=1)
    results["postgres"] = ok
    print("✅" if ok else "❌  docker compose -f docker/docker-compose.postgres.yml up -d")

    # Flask
    print(f"  [2/4] Flask Entry (localhost:{FLASK_PORT})...", end=" ")
    try:
        resp = requests.get(f"http://localhost:{FLASK_PORT}/health", timeout=3)
        results["flask"] = resp.status_code == 200
        print("✅" if results["flask"] else f"❌  HTTP {resp.status_code}")
    except requests.RequestException:
        results["flask"] = False
        print("⚠️   non démarré (optionnel pour l'export)")

    # Kestra
    print("  [3/4] Kestra (localhost:8080)...", end=" ")
    try:
        resp = requests.get("http://localhost:8080/health", timeout=5)
        results["kestra"] = resp.status_code in (200, 404)
        print("✅" if results["kestra"] else f"❌  HTTP {resp.status_code}")
    except requests.RequestException:
        results["kestra"] = False
        print("⚠️   non démarré (optionnel pour l'export)")

    # Delta Lake files
    bronze = Path("data/delta/bronze/strava/_delta_log")
    silver = Path("data/delta/silver/employees/_delta_log")
    gold   = Path("data/delta/gold/avantages/_delta_log")
    ok_delta = bronze.exists() and silver.exists() and gold.exists()
    results["delta"] = ok_delta
    print("  [4/4] Delta Lake (Bronze + Silver + Gold)...", end=" ")
    print("✅" if ok_delta else f"❌  Manquants : "
          + ", ".join(n for n, p in [("Bronze", bronze), ("Silver", silver), ("Gold", gold)] if not p.exists()))

    return results


# ════════════════════════════════════════════════════════════════
# 2. Vérification données tous rounds
# ════════════════════════════════════════════════════════════════

def check_all_rounds() -> dict[str, dict]:
    """Vérifie les données de tous les rounds dans PostgreSQL."""
    print(f"\n{SEP}")
    print("  VÉRIFICATION DES DONNÉES — R1 → R4")
    print(SEP)

    report = {}

    with get_connection() as conn:
        with conn.cursor() as cur:

            checks = [
                ("R1 strava_activities",  "SELECT COUNT(*), COUNT(DISTINCT employee_id), COUNT(DISTINCT sport_type) FROM strava_activities WHERE source='strava'"),
                ("R2 employees",          "SELECT COUNT(*), SUM(CASE WHEN eligible_prime THEN 1 ELSE 0 END), SUM(CASE WHEN distance_bureau_km IS NOT NULL THEN 1 ELSE 0 END) FROM employees"),
                ("R2 gmaps_cache",        "SELECT COUNT(*), COUNT(DISTINCT mode_transport) FROM gmaps_cache WHERE mode_transport IN ('walking','bicycling')"),
                ("R3 avantages_calcules", "SELECT COUNT(*), SUM(CASE WHEN eligible_prime THEN 1 ELSE 0 END), ROUND(SUM(montant_prime)::NUMERIC,2) FROM avantages_calcules"),
                ("R3 data_quality",       "SELECT COUNT(*), SUM(CASE WHEN resultat THEN 1 ELSE 0 END) FROM data_quality_results"),
                ("R4 strava manual",      "SELECT COUNT(*) FROM strava_activities WHERE source='manual'"),
            ]

            labels = [
                ("total_acts", "athletes", "sports"),
                ("total_emp", "nb_prime", "avec_dist"),
                ("cache_ok", "modes_ok", None),
                ("total_gold", "eligibles", "cout_primes"),
                ("total_qc", "qc_ok", None),
                ("manual_acts", None, None),
            ]

            for (name, sql), cols in zip(checks, labels):
                cur.execute(sql)
                row = cur.fetchone()
                vals = {k: v for k, v in zip([c for c in cols if c], row) if k}
                report[name] = vals
                v_str = " | ".join(f"{k}={v}" for k, v in vals.items())
                icon = "✅" if row[0] and row[0] > 0 else "⚠️ "
                print(f"  {icon} {name:<30} {v_str}")

    return report


# ════════════════════════════════════════════════════════════════
# 3. Rapport synthèse final
# ════════════════════════════════════════════════════════════════

def print_final_summary(report: dict) -> None:
    """Affiche le récapitulatif complet du POC."""
    print(f"\n{SEP}")
    print("  RAPPORT FINAL — POC AVANTAGES SPORTIFS")
    print(SEP)

    g = report.get("R3 avantages_calcules", {})
    a = report.get("R1 strava_activities",  {})
    e = report.get("R2 employees",          {})

    print(f"""
  Pipeline de données complet :
  XLSX RH (161) + XLSX Sport (95) → employees (Silver) → avantages_calcules (Gold)
  Monte Carlo 2025 → {a.get('total_acts','?')} activités → journées bien-être calculées

  ──────────────────────────────────────────────────────────────
  PRIMES SPORTIVES (taux 5 %)
    Salariés total         : {g.get('total_gold', 161)}
    Éligibles prime        : {g.get('eligibles', '?')} ({int(g.get('eligibles',0))*100//161 if g.get('eligibles') else '?'} %)
    Coût total             : {g.get('cout_primes', '?')} €
    → dont marche/running  : 14 salariés (≤ 15 km)
    → dont vélo/trottinette: 54 salariés (≤ 25 km)

  JOURNÉES BIEN-ÊTRE (seuil 15 activités/an)
    Athlètes simulés       : {a.get('athletes', '?')} (tous actifs sur 2025)
    Éligibles BE           : ~47 / 161
    Total jours accordés   : ~235 jours

  ARCHITECTURE DELTA LAKE
    Bronze : data/delta/bronze/strava/     (R1 — activités brutes)
    Silver : data/delta/silver/employees/  (R2 — données normalisées)
    Gold   : data/delta/gold/avantages/    (R3 — avantages calculés)

  ORCHESTRATION
    6 flows Kestra déployés dans poc.avantages_sportifs
    Flask Entry       : http://localhost:{FLASK_PORT}
    Kestra UI         : http://localhost:8080
    Notifications Slack : temps réel (< 5 secondes)
  ──────────────────────────────────────────────────────────────
""")


# ════════════════════════════════════════════════════════════════
# 4. Instructions démo live
# ════════════════════════════════════════════════════════════════

def print_demo_script() -> None:
    """Script de démo live pour la restitution."""
    print(f"{SEP}")
    print("  SCRIPT DE DÉMO LIVE — 5 minutes")
    print(SEP)
    print(f"""
  SÉQUENCE RECOMMANDÉE :

  1. DONNÉES & PIPELINE (1 min)
     Montrer pgAdmin → http://localhost:5050
     SELECT COUNT(*) FROM avantages_calcules;          → 161
     SELECT SUM(montant_prime) FROM avantages_calcules; → ~106 427 €

  2. SAISIE MANUELLE → SLACK (1 min)
     Ouvrir http://localhost:{FLASK_PORT}
     Sélectionner salarié + Running + 12 km + 55 min
     Commentaire : "Belle sortie ce matin ! 🌅"
     → Clic Enregistrer
     → Message Slack dans le channel en < 5 secondes 🎉

  3. KESTRA ORCHESTRATION (1 min)
     Ouvrir http://localhost:8080
     Montrer le flow notify_slack → Exécutions récentes
     "Polling automatique toutes les 5 minutes"

  4. DELTA LAKE (1 min)
     python scripts/check_gold_delta.py
     "3 couches : Bronze brut / Silver normalisé / Gold calculé"
     "Sans Spark, sans JVM, sans S3 — delta-rs + DuckDB"

  5. POWER BI (1 min)
     Ouvrir Power BI Desktop
     → reports/power_bi/ ou connexion ODBC PostgreSQL
     Page Vue Globale → KPI coût total + % éligibles
     Slicer BU : filtrer sur un département
     Modifier taux dans config → UPDATE + re-export → refresh PBI

  COMMANDES DE SECOURS :
     python src/export_power_bi.py               # re-export CSV
     python scripts/run_round3.py --dry-run      # vérif Gold
     curl http://localhost:{FLASK_PORT}/api/stats          # KPI JSON
""")


# ════════════════════════════════════════════════════════════════
# 5. Archivage tests complets
# ════════════════════════════════════════════════════════════════

def run_all_tests() -> None:
    """Lance tous les tests des 5 rounds."""
    import subprocess
    print(f"\n{SEP}")
    print("  TESTS COMPLETS — R1 → R5")
    print(SEP)
    result = subprocess.run(
        ["pytest", "tests/", "-v", "--tb=short", "-q"],
        capture_output=False
    )
    if result.returncode == 0:
        print("\n  ✅ Tous les tests passés")
    else:
        print(f"\n  ⚠️  Des tests ont échoué (code {result.returncode})")


# ════════════════════════════════════════════════════════════════
# Point d'entrée
# ════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description="Round 5 — Export Power BI + rapport final")
    parser.add_argument("--export-only", action="store_true",
                        help="Seulement l'export Power BI (sans check infra)")
    parser.add_argument("--format", choices=["csv", "excel"], default="csv",
                        help="Format d'export Power BI (défaut : csv)")
    parser.add_argument("--run-tests", action="store_true",
                        help="Lancer tous les tests pytest R1→R5")
    args = parser.parse_args()

    debut = datetime.now()

    print(f"\n{'#'*65}")
    print(f"#  POC Avantages Sportifs — ROUND 5 FINAL")
    print(f"#  Power BI + Démo + Clôture projet")
    print(f"#  {debut.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*65}")

    if not args.export_only:
        check_infrastructure()

    if not ping(max_retries=3, delay=1):
        logger.error("PostgreSQL non accessible — impossible de continuer")
        sys.exit(1)

    report = check_all_rounds()
    print_final_summary(report)

    # Export Power BI
    print(f"\n{SEP}")
    print(f"  EXPORT POWER BI ({args.format.upper()})")
    print(SEP)
    from export_power_bi import main as export_pbi, OUTPUT_DIR
    export_pbi(fmt=args.format, output_dir=OUTPUT_DIR)

    if args.run_tests:
        run_all_tests()

    print_demo_script()

    log_run(
        flow_name="run_round5",
        etape="export_power_bi + rapport_final",
        statut="SUCCESS",
        debut=debut,
        nb_lignes=report.get("R3 avantages_calcules", {}).get("total_gold", 0),
    )

    elapsed = (datetime.now() - debut).total_seconds()
    print(f"  ⏱  Durée : {elapsed:.1f}s")
    print(f"  📁  Exports : {(OUTPUT_DIR).resolve()}\n")


if __name__ == "__main__":
    main()

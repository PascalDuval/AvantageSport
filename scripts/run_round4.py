"""
Script de setup et vérification — Round 4

Vérifie que tous les composants du Round 4 sont opérationnels :
  1. PostgreSQL accessible
  2. Flask Entry accessible (port 5001)
  3. Kestra accessible (port 8080)
  4. Fichiers flows YAML présents
  5. SLACK_WEBHOOK_URL configuré
  6. Test d'envoi Slack (dry run)

Usage:
    python scripts/run_round4.py
    python scripts/run_round4.py --start-flask   # démarre Flask en arrière-plan
    python scripts/run_round4.py --test-slack    # test d'envoi Slack réel
"""
import argparse
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database import ping, get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SEP = "=" * 65
FLASK_PORT = int(os.getenv("FLASK_PORT", "5001"))
KESTRA_URL = os.getenv("KESTRA_URL", "http://localhost:8080")
KESTRA_NAMESPACE = "poc.avantages_sportifs"


def check_all() -> dict[str, bool]:
    """Vérifie tous les prérequis du Round 4."""
    print(f"\n{SEP}")
    print("  CHECK ROUND 4 — Orchestration Kestra + Flask + Slack")
    print(SEP)

    results = {}

    # 1. PostgreSQL
    print("\n  [1/6] PostgreSQL...", end=" ")
    ok = ping(max_retries=3, delay=1)
    results["postgres"] = ok
    print("✅" if ok else "❌  docker compose -f docker/docker-compose.postgres.yml up -d")

    # 2. Rounds précédents (données)
    print("  [2/6] Données (employees + avantages_calcules)...", end=" ")
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM employees")
                nb_e = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM avantages_calcules")
                nb_a = cur.fetchone()[0]
        ok_data = nb_e > 0 and nb_a > 0
        results["data"] = ok_data
        print(f"✅  ({nb_e} employees, {nb_a} avantages)" if ok_data
              else f"❌  employees={nb_e} / avantages={nb_a} — lancez les rounds 1→3 d'abord")
    except Exception as e:
        results["data"] = False
        print(f"❌  {e}")

    # 3. Flask Entry
    print(f"  [3/6] Flask Entry (port {FLASK_PORT})...", end=" ")
    try:
        resp = requests.get(f"http://localhost:{FLASK_PORT}/health", timeout=3)
        ok_flask = resp.status_code == 200
        results["flask"] = ok_flask
        print("✅" if ok_flask else f"❌  HTTP {resp.status_code}")
    except requests.RequestException:
        results["flask"] = False
        print(f"❌  Non accessible — lancez : python src/flask_entry.py")

    # 4. Kestra
    print(f"  [4/6] Kestra ({KESTRA_URL})...", end=" ")
    try:
        resp = requests.get(f"{KESTRA_URL}/api/v1/flows", timeout=5)
        ok_kestra = resp.status_code in (200, 401)  # 401 = auth requise = Kestra tourne
        results["kestra"] = ok_kestra
        print("✅" if ok_kestra else f"❌  HTTP {resp.status_code}")
    except requests.RequestException:
        results["kestra"] = False
        print(f"❌  Non accessible sur {KESTRA_URL}")

    # 5. Flows YAML présents
    flows_dir = Path("kestra/flows")
    expected_flows = [
        "01_ingest_xlsx.yml",
        "02_generate_strava.yml",
        "03_etl_silver_gold.yml",
        "04_quality_check.yml",
        "05_notify_slack.yml",
    ]
    print(f"  [5/6] Flows Kestra YAML...", end=" ")
    missing = [f for f in expected_flows if not (flows_dir / f).exists()]
    ok_flows = not missing
    results["flows"] = ok_flows
    if ok_flows:
        print(f"✅  ({len(expected_flows)} flows présents dans kestra/flows/)")
    else:
        print(f"❌  Manquants : {missing}")

    # 6. Slack webhook
    print("  [6/6] Slack Webhook URL...", end=" ")
    webhook = os.getenv("SLACK_WEBHOOK_URL", "")
    ok_slack = bool(webhook) and webhook.startswith("https://hooks.slack.com/")
    results["slack"] = ok_slack
    if ok_slack:
        print("✅  (URL configurée)")
    else:
        print("⚠️   SLACK_WEBHOOK_URL absent ou invalide dans .env → mode DRY RUN")

    print()
    return results


def test_flask_endpoints() -> None:
    """Teste chaque endpoint Flask avec un appel réel."""
    print(f"\n{SEP}")
    print("  TEST ENDPOINTS FLASK")
    print(SEP)

    base = f"http://localhost:{FLASK_PORT}"
    endpoints = [
        ("GET",  "/health",     None,              "Health check"),
        ("GET",  "/api/stats",  None,              "KPI Gold"),
        ("POST", "/api/notify", '{"dry_run":true}', "Slack notifier (dry run)"),
    ]

    for method, path, body, label in endpoints:
        try:
            if method == "GET":
                resp = requests.get(f"{base}{path}", timeout=10)
            else:
                resp = requests.post(
                    f"{base}{path}", data=body,
                    headers={"Content-Type": "application/json"}, timeout=30
                )
            icon = "✅" if resp.status_code < 400 else "❌"
            print(f"  {icon} {method} {path:<20} HTTP {resp.status_code} — {label}")
        except requests.RequestException as e:
            print(f"  ❌ {method} {path:<20} Erreur réseau : {e}")


def print_kestra_instructions() -> None:
    """Affiche les instructions pour déployer les flows dans Kestra."""
    print(f"\n{SEP}")
    print("  DÉPLOIEMENT DES FLOWS DANS KESTRA")
    print(SEP)
    flows_dir = Path("kestra/flows")

    print(f"\n  Option A — Via l'interface Kestra (http://localhost:8080)")
    print(f"  1. Flows → + Create")
    print(f"  2. Coller le contenu de chaque fichier YAML")
    print(f"  3. Save → Execute pour tester")

    print(f"\n  Option B — Via l'API Kestra (curl ou requests)")
    for f in sorted(flows_dir.glob("*.yml")):
        print(f"  curl -X POST {KESTRA_URL}/api/v1/flows \\")
        print(f"       -H 'Content-Type: application/x-yaml' \\")
        print(f"       --data-binary @{f}")
        print()

    print(f"  Namespace : {KESTRA_NAMESPACE}")
    print(f"  Flows     : {', '.join(f.stem for f in sorted(flows_dir.glob('*.yml')))}")


def print_demo_instructions() -> None:
    """Instructions pour la démo live."""
    print(f"\n{SEP}")
    print("  DÉMO LIVE — Flask → Slack en < 10 secondes")
    print(SEP)
    print(f"""
  1. Démarrer Flask (terminal 1) :
     conda activate datascience2
     python src/flask_entry.py

  2. Ouvrir http://localhost:5001 dans le navigateur

  3. Sélectionner un salarié + sport + durée → Enregistrer

  4. Le message Slack apparaît dans le channel en < 5 secondes 🎉

  Ou via curl :
  curl -X POST http://localhost:5001/api/activity \\
       -H "Content-Type: application/json" \\
       -d '{{"employee_id":"12345","sport_type":"Running","duree_min":46,"distance_km":10.8,"commentaire":"Belle sortie !"}}' 
""")


def main() -> None:
    parser = argparse.ArgumentParser(description="Setup et vérification — Round 4")
    parser.add_argument("--start-flask", action="store_true",
                        help="Démarre Flask en arrière-plan")
    parser.add_argument("--test-endpoints", action="store_true",
                        help="Teste tous les endpoints Flask")
    parser.add_argument("--test-slack", action="store_true",
                        help="Test d'envoi Slack réel (nécessite SLACK_WEBHOOK_URL)")
    args = parser.parse_args()

    # Démarrer Flask si demandé
    if args.start_flask:
        logger.info(f"Démarrage de Flask sur :5001...")
        subprocess.Popen(
            [sys.executable, "src/flask_entry.py", "--port", str(FLASK_PORT)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        time.sleep(2)
        logger.info("Flask démarré en arrière-plan")

    results = check_all()

    if args.test_endpoints and results.get("flask"):
        test_flask_endpoints()

    if args.test_slack and results.get("flask"):
        print(f"\n{SEP}")
        print("  TEST SLACK (activité fictive)")
        print(SEP)
        resp = requests.post(
            f"http://localhost:{FLASK_PORT}/api/notify",
            json={"since_minutes": 60, "dry_run": not results.get("slack")},
            timeout=15,
        )
        print(f"  Résultat : {resp.json()}")

    print_kestra_instructions()
    print_demo_instructions()

    all_critical = all(results.get(k) for k in ["postgres", "data"])
    if all_critical:
        print(f"\n  ✅ Setup Round 4 validé")
    else:
        print(f"\n  ⚠️  Certains prérequis non satisfaits — voir ci-dessus")


if __name__ == "__main__":
    main()

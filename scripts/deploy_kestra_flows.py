"""
Deploy Kestra Flows

Pousse tous les flows YAML vers Kestra via son API REST.
Utilise PUT /api/v1/flows/{namespace}/{id} pour créer ou mettre à jour.

Usage:
    # Pousser tous les flows (01 → 05 + 00)
    python scripts/deploy_kestra_flows.py

    # Pousser uniquement le flow 04
    python scripts/deploy_kestra_flows.py --flow 04_quality_check

    # Pousser vers un Kestra distant
    python scripts/deploy_kestra_flows.py --kestra-url http://mon-kestra:8080

Prérequis :
    - Kestra démarré : docker compose -f docker/docker-compose.kestra.yml up -d
    - Flask Entry démarré : python src/flask_entry.py
    - pip install requests (déjà dans l'env datascience2)
"""
import argparse
import logging
import os
import sys
import time
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROOT_DIR   = Path(__file__).resolve().parent.parent
FLOWS_DIR  = ROOT_DIR / "kestra" / "flows"
NAMESPACE  = "poc.avantages_sportifs"
TENANT_ID  = "main"   # Kestra OSS tenant par défaut

# Ordre de déploiement : flows individuels d'abord, puis le flow maître
FLOW_ORDER = [
    "01_ingest_xlsx.yml",
    "02_generate_strava.yml",
    "03_etl_silver_gold.yml",
    "04_quality_check.yml",
    "05_notify_slack.yml",
    "00_pipeline_complet.yml",   # maître en dernier (référence les autres)
]

SEP  = "═" * 65
SEP2 = "─" * 65


def _build_auth_headers(token: str | None, user: str | None, password: str | None) -> dict:
    """Construit les headers d'authentification."""
    headers: dict[str, str] = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    elif user and password:
        import base64
        cred = base64.b64encode(f"{user}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {cred}"
    return headers


def wait_for_kestra(base_url: str, auth_headers: dict, max_wait: int = 60) -> bool:
    """Attend que Kestra soit disponible (max_wait secondes)."""
    url = f"{base_url}/api/v1/{TENANT_ID}/flows/search?namespace={NAMESPACE}&size=1"
    for attempt in range(1, max_wait // 3 + 1):
        try:
            resp = requests.get(url, headers=auth_headers, timeout=5)
            if resp.status_code < 500:
                logger.info(f"  ✅ Kestra accessible ({base_url})")
                return True
        except requests.exceptions.ConnectionError:
            pass
        logger.info(f"  ⏳ Kestra pas encore prêt (tentative {attempt})…")
        time.sleep(3)
    logger.error(f"  ❌ Kestra inaccessible après {max_wait}s — vérifiez Docker")
    return False


def deploy_flow(flow_path: Path, base_url: str, auth_headers: dict) -> bool:
    """
    Pousse un fichier flow YAML vers Kestra.

    Kestra API (avec tenant) :
      PUT /api/v1/{tenant}/flows/{namespace}/{id}

    Auth : Bearer token (--token) ou Basic Auth (--user/--password).
    Pour créer un token API dans Kestra : Settings → API Tokens → Create.

    Returns:
        True si déployé avec succès.
    """
    yaml_content = flow_path.read_text(encoding="utf-8")

    # Extraction de l'id du flow depuis le YAML (première ligne "id: <id>")
    flow_id = None
    for line in yaml_content.splitlines():
        stripped = line.strip()
        if stripped.startswith("id:") and not stripped.startswith("id: #"):
            flow_id = stripped.split(":", 1)[1].strip()
            break

    if not flow_id:
        logger.error(f"  ❌ Impossible d'extraire l'id du flow : {flow_path.name}")
        return False

    headers = {**auth_headers, "Content-Type": "application/x-yaml"}
    url = f"{base_url}/api/v1/{TENANT_ID}/flows/{NAMESPACE}/{flow_id}"

    try:
        resp = requests.put(url, data=yaml_content.encode("utf-8"), headers=headers, timeout=15)
        if resp.status_code in (200, 201):
            action = "créé" if resp.status_code == 201 else "mis à jour"
            logger.info(f"  ✅ [{resp.status_code}] {flow_path.name} → {action} (id={flow_id})")
            return True
        elif resp.status_code == 401:
            logger.error(
                f"  ❌ [401] Authentification requise.\n"
                f"         Utilisez --token <API_TOKEN> ou --user <login> --password <mdp>\n"
                f"         Token Kestra : http://localhost:8080 → Settings → API Tokens → Create"
            )
            return False
        else:
            logger.error(f"  ❌ [{resp.status_code}] {flow_path.name} — {resp.text[:300]}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"  ❌ Erreur réseau pour {flow_path.name} : {e}")
        return False


def deploy_all(
    base_url: str,
    auth_headers: dict,
    only_flow: str | None = None,
) -> dict[str, bool]:
    """
    Déploie tous les flows (ou uniquement celui spécifié).

    Returns:
        dict {nom_fichier: succès}
    """
    results: dict[str, bool] = {}

    # Sélection des flows à déployer
    if only_flow:
        # Cherche le fichier par nom partiel
        matches = [f for f in FLOWS_DIR.glob("*.yml") if only_flow in f.stem]
        if not matches:
            logger.error(f"Aucun flow trouvé correspondant à '{only_flow}'")
            return {}
        files_to_deploy = matches
    else:
        # Ordre défini — flows manquants ignorés avec warning
        files_to_deploy = []
        for fname in FLOW_ORDER:
            path = FLOWS_DIR / fname
            if path.exists():
                files_to_deploy.append(path)
            else:
                logger.warning(f"  ⚠️  Flow absent (ignoré) : {fname}")

    logger.info(f"\n{SEP}")
    logger.info(f"  DÉPLOIEMENT KESTRA — {len(files_to_deploy)} flow(s)")
    logger.info(f"  Namespace : {NAMESPACE}")
    logger.info(f"  URL Kestra : {base_url}")
    logger.info(SEP)

    for path in files_to_deploy:
        results[path.name] = deploy_flow(path, base_url, auth_headers)

    return results


def print_summary(results: dict[str, bool]) -> None:
    """Affiche le résumé du déploiement."""
    nb_ok  = sum(1 for v in results.values() if v)
    nb_ko  = len(results) - nb_ok

    logger.info(f"\n{SEP2}")
    logger.info(f"  RÉSUMÉ : {nb_ok}/{len(results)} flows déployés")
    if nb_ko:
        logger.warning(f"  ⚠️  {nb_ko} flow(s) en échec :")
        for name, ok in results.items():
            if not ok:
                logger.warning(f"     - {name}")
    else:
        logger.info("  ✅ Tous les flows ont été déployés avec succès.")
    logger.info(f"  Kestra UI : http://localhost:8080")
    logger.info(f"  Namespace : {NAMESPACE}")
    logger.info(SEP2)


def main():
    parser = argparse.ArgumentParser(
        description="Déploiement des flows Kestra",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Exemples :\n"
            "  # Token API (recommandé — Kestra → Settings → API Tokens)\n"
            "  python scripts/deploy_kestra_flows.py --token <mon_token>\n\n"
            "  # Variable d'environnement\n"
            "  $env:KESTRA_API_TOKEN='<mon_token>'\n"
            "  python scripts/deploy_kestra_flows.py\n\n"
            "  # Basic Auth (si configuré dans Kestra)\n"
            "  python scripts/deploy_kestra_flows.py --user admin --password mon_mdp\n\n"
            "  # Flow unique\n"
            "  python scripts/deploy_kestra_flows.py --token <t> --flow 04_quality_check\n"
        ),
    )
    parser.add_argument("--kestra-url", default="http://localhost:8080",
                        help="URL Kestra (défaut : http://localhost:8080)")
    parser.add_argument("--flow", default=None,
                        help="Nom partiel d'un flow à déployer seul")
    parser.add_argument("--token", default=os.getenv("KESTRA_API_TOKEN"),
                        help="Token API Kestra (ou $KESTRA_API_TOKEN)")
    parser.add_argument("--user", default=os.getenv("KESTRA_USER"),
                        help="Identifiant Basic Auth (ou $KESTRA_USER)")
    parser.add_argument("--password", default=os.getenv("KESTRA_PASSWORD"),
                        help="Mot de passe Basic Auth (ou $KESTRA_PASSWORD)")
    parser.add_argument("--no-wait", action="store_true",
                        help="Ne pas vérifier la disponibilité de Kestra avant de déployer")
    args = parser.parse_args()

    auth_headers = _build_auth_headers(args.token, args.user, args.password)

    if not auth_headers:
        logger.warning(
            "  ⚠️  Aucune authentification fournie.\n"
            "     Si Kestra retourne 401 :\n"
            "       → Kestra UI → Settings → API Tokens → Create\n"
            "       → python scripts/deploy_kestra_flows.py --token <votre_token>"
        )

    if not args.no_wait:
        if not wait_for_kestra(args.kestra_url, auth_headers):
            sys.exit(1)

    results = deploy_all(base_url=args.kestra_url, auth_headers=auth_headers, only_flow=args.flow)
    if not results:
        sys.exit(1)

    print_summary(results)
    all_ok = all(results.values())
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()

"""
Slack Notifier — Round 4

Envoie des notifications Slack pour les activités sportives.

Deux modes de déclenchement :
  1. Immédiat   : appelé par Flask juste après une saisie manuelle
  2. Polling    : appelé par Kestra (schedule) — scrute les nouvelles
                  activités depuis la dernière vérification

Le webhook Slack est configuré dans .env (SLACK_WEBHOOK_URL).
Si la variable est absente, le module tourne en DRY RUN
(affiche le message sans l'envoyer) — utile pour les tests.

Usage:
    python src/slack_notifier.py                      # poll mode (5 dernières minutes)
    python src/slack_notifier.py --minutes 10         # poll sur 10 minutes
    python src/slack_notifier.py --activity-id 42     # notifier une activité précise
    python src/slack_notifier.py --dry-run            # affiche sans envoyer
"""
import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import psycopg2.extras
import requests

sys.path.insert(0, str(Path(__file__).parent))
from database import get_connection, log_run, ping

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/slack_notifier.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ── Constantes d'émoji par sport ─────────────────────────────────
SPORT_EMOJIS: dict[str, str] = {
    "Running":         "🏃",
    "Runing":          "🏃",
    "Randonnée":       "🏔",
    "Natation":        "🏊",
    "Vélo":            "🚴",
    "Tennis":          "🎾",
    "Football":        "⚽",
    "Rugby":           "🏉",
    "Basketball":      "🏀",
    "Badminton":       "🏸",
    "Judo":            "🥋",
    "Boxe":            "🥊",
    "Escalade":        "🧗",
    "Triathlon":       "🏅",
    "Voile":           "⛵",
    "Équitation":      "🐎",
    "Tennis de table": "🏓",
}

# ── Templates de messages par sport ──────────────────────────────
MESSAGES_TEMPLATES: dict[str, list[str]] = {
    "Running":   [
        "Tu viens de courir {dist} en {dur} ! Quelle énergie ! 🔥",
        "{dist} avalés en {dur} ! Continue comme ça ! 💪",
        "Une belle course de {dist} en {dur} ! 🏅",
    ],
    "Randonnée": [
        "Une randonnée de {dist} terminée en {dur} ! 🌄",
        "{dist} de sentiers parcourus en {dur} — splendide ! 🏕",
        "Nouvelle aventure : {dist} en {dur}. Un nouveau spot découvert ? 🗺",
    ],
    "Natation":  [
        "{dist} dans l'eau en {dur} ! Bravo la nage ! 🌊",
        "Superbe séance de natation : {dist} en {dur} ! 🏊",
    ],
    "Vélo":      [
        "{dist} à vélo en {dur} ! Les mollets sont chauds ! 🚴",
        "Belle sortie vélo : {dist} en {dur}. La forme ! 🔥",
    ],
    "_default":  [
        "Séance de {sport} terminée en {dur} ! Bravo ! 💪",
        "Belle performance : {sport} pendant {dur} ! 🏅",
        "{sport} : {dur} de pur effort ! 🔥",
    ],
}


# ════════════════════════════════════════════════════════════════
# 1. Formatage des messages
# ════════════════════════════════════════════════════════════════

def _fmt_distance(distance_m: Optional[float]) -> str:
    """
    Formate une distance en mètres en chaîne lisible.

    Args:
        distance_m: distance en mètres, ou None.

    Returns:
        str: ex. '10,8 km' ou '' si absent.
    """
    if distance_m is None or distance_m <= 0:
        return ""
    km = distance_m / 1000
    return f"{km:.1f} km".replace(".", ",")


def _fmt_duration(duree_s: Optional[int]) -> str:
    """
    Formate une durée en secondes en chaîne lisible.

    Args:
        duree_s: durée en secondes.

    Returns:
        str: ex. '46 min' ou '1h 23min'.
    """
    if duree_s is None or duree_s <= 0:
        return "?"
    minutes = duree_s // 60
    if minutes < 60:
        return f"{minutes} min"
    h, m = divmod(minutes, 60)
    return f"{h}h {m:02d}min" if m > 0 else f"{h}h"


def format_message(
    activity: dict,
    employee: dict,
) -> str:
    """
    Formate le message Slack pour une activité donnée.

    Format cible (exemple README) :
        🏅 Bravo Juliette Mendes ! Tu viens de courir 10,8 km en 46 min ! Quelle énergie ! 🔥

    Args:
        activity: dict avec les champs de strava_activities
                  (sport_type, distance_m, duree_s, commentaire).
        employee: dict avec les champs de employees (prenom, nom, bu).

    Returns:
        str: message Slack formaté.
    """
    import random

    prenom = str(employee.get("prenom", "")).strip()
    nom    = str(employee.get("nom",    "")).strip()
    sport  = str(activity.get("sport_type", "Sport")).strip()
    emoji  = SPORT_EMOJIS.get(sport, "💪")

    dist_str = _fmt_distance(activity.get("distance_m"))
    dur_str  = _fmt_duration(activity.get("duree_s"))

    # Choisir un template
    templates = MESSAGES_TEMPLATES.get(sport, MESSAGES_TEMPLATES["_default"])
    tpl = random.choice(templates)

    # Remplir le template
    phrase = tpl.format(
        dist=dist_str if dist_str else dur_str,
        dur=dur_str,
        sport=sport,
    )

    # Construire le message complet
    parts = [f"{emoji} Bravo {prenom} {nom} ! {phrase}"]

    # Ajouter le commentaire si présent
    commentaire = activity.get("commentaire")
    if commentaire and str(commentaire).strip() not in ("", "None"):
        parts.append(f'_"{commentaire.strip()}"_')

    return "\n".join(parts)


def build_slack_payload(message: str, activity: dict, employee: dict) -> dict:
    """
    Construit le payload JSON pour l'API Slack (Incoming Webhook).

    Utilise les Block Kit de Slack pour un message riche.

    Args:
        message:  texte principal formaté.
        activity: dict de l'activité.
        employee: dict du salarié.

    Returns:
        dict: payload JSON prêt à être envoyé via requests.post.
    """
    sport  = activity.get("sport_type", "Sport")
    emoji  = SPORT_EMOJIS.get(sport, "💪")
    bu     = employee.get("bu", "")

    fields = []
    if activity.get("distance_m"):
        fields.append({
            "type": "mrkdwn",
            "text": f"*Distance*\n{_fmt_distance(activity['distance_m'])}"
        })
    if activity.get("duree_s"):
        fields.append({
            "type": "mrkdwn",
            "text": f"*Durée*\n{_fmt_duration(activity['duree_s'])}"
        })
    if bu:
        fields.append({
            "type": "mrkdwn",
            "text": f"*BU*\n{bu}"
        })

    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": message}
        }
    ]
    if fields:
        blocks.append({"type": "section", "fields": fields})

    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": f"{emoji} *{sport}* · POC Avantages Sportifs"
        }]
    })

    return {"blocks": blocks, "text": message}


# ════════════════════════════════════════════════════════════════
# 2. Envoi du message
# ════════════════════════════════════════════════════════════════

def send_slack_message(
    activity: dict,
    employee: dict,
    dry_run: bool = False,
) -> bool:
    """
    Envoie un message Slack pour une activité.

    Args:
        activity: dict de l'activité (sport_type, distance_m, duree_s…).
        employee: dict du salarié (prenom, nom, bu…).
        dry_run:  si True, affiche le message sans l'envoyer.

    Returns:
        bool: True si le message a été envoyé (ou simulé en dry_run).
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    message = format_message(activity, employee)

    if dry_run or not webhook_url:
        mode = "DRY RUN" if dry_run else "NO WEBHOOK (simulation)"
        logger.info(f"💬 Slack [{mode}] :\n{message}\n")
        return True

    payload = build_slack_payload(message, activity, employee)

    try:
        resp = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200 and resp.text == "ok":
            logger.info(f"✅ Slack envoyé : {activity.get('sport_type')} / {employee.get('nom')}")
            return True
        else:
            logger.error(f"❌ Slack KO : HTTP {resp.status_code} — {resp.text[:200]}")
            return False
    except requests.RequestException as e:
        logger.error(f"❌ Erreur réseau Slack : {e}")
        return False


# ════════════════════════════════════════════════════════════════
# 3. Modes de déclenchement
# ════════════════════════════════════════════════════════════════

def notify_by_activity_id(
    activity_id: int,
    dry_run: bool = False,
) -> bool:
    """
    Notifie Slack pour une activité précise (appelé par Flask après saisie manuelle).

    Args:
        activity_id: id de strava_activities.
        dry_run:     si True, affiche sans envoyer.

    Returns:
        bool: True si envoyé avec succès.
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            cur.execute(
                """
                SELECT sa.*, e.nom, e.prenom, e.bu
                FROM strava_activities sa
                LEFT JOIN employees e ON e.id = sa.employee_id
                WHERE sa.id = %s
                """,
                (activity_id,),
            )
            row = cur.fetchone()

    if not row:
        logger.warning(f"Activité {activity_id} non trouvée")
        return False

    activity = dict(row)
    employee = {
        "nom":    activity.pop("nom", "Salarié"),
        "prenom": activity.pop("prenom", ""),
        "bu":     activity.pop("bu", ""),
    }
    return send_slack_message(activity, employee, dry_run=dry_run)


def poll_and_notify(
    since_minutes: int = 5,
    dry_run: bool = False,
) -> int:
    """
    Scrute les nouvelles activités créées dans les X dernières minutes et notifie Slack.

    Appelé par Kestra en mode scheduling (ex : toutes les 5 minutes).
    La fenêtre glissante évite les doublons et les oublis.

    Args:
        since_minutes: fenêtre de polling en minutes (défaut : 5).
        dry_run:       si True, affiche sans envoyer.

    Returns:
        int: nombre de notifications envoyées.
    """
    since = datetime.now() - timedelta(minutes=since_minutes)

    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT sa.*, e.nom, e.prenom, e.bu
                FROM strava_activities sa
                LEFT JOIN employees e ON e.id = sa.employee_id
                WHERE sa.created_at >= %s
                AND sa.source = 'manual'
                ORDER BY sa.created_at ASC
                """,
                (since,),
            )
            rows = cur.fetchall()

    if not rows:
        logger.info(f"Aucune nouvelle activité manuelle dans les {since_minutes} dernières minutes")
        return 0

    logger.info(f"📬 {len(rows)} activité(s) à notifier...")
    sent = 0
    for row in rows:
        activity = dict(row)
        employee = {
            "nom":    activity.pop("nom", "Salarié"),
            "prenom": activity.pop("prenom", ""),
            "bu":     activity.pop("bu", ""),
        }
        if send_slack_message(activity, employee, dry_run=dry_run):
            sent += 1

    logger.info(f"✅ {sent}/{len(rows)} notifications Slack envoyées")
    return sent


# ════════════════════════════════════════════════════════════════
# 4. Point d'entrée
# ════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description="Notificateur Slack — Round 4")
    parser.add_argument("--minutes", type=int, default=5,
                        help="Fenêtre de polling en minutes (défaut : 5)")
    parser.add_argument("--activity-id", type=int, default=None,
                        help="Notifier une activité précise par son ID")
    parser.add_argument("--dry-run", action="store_true",
                        help="Affiche les messages sans les envoyer")
    args = parser.parse_args()

    if not ping(max_retries=3, delay=1):
        sys.exit(1)

    debut = datetime.now()

    if args.activity_id:
        ok = notify_by_activity_id(args.activity_id, dry_run=args.dry_run)
        nb = 1 if ok else 0
    else:
        nb = poll_and_notify(since_minutes=args.minutes, dry_run=args.dry_run)

    log_run(
        flow_name="notify_slack",
        etape="poll_and_notify" if not args.activity_id else "notify_by_id",
        statut="SUCCESS",
        debut=debut,
        nb_lignes=nb,
        metadata={"since_minutes": args.minutes, "dry_run": args.dry_run},
    )


if __name__ == "__main__":
    main()

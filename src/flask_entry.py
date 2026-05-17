"""
Flask Entry — Round 4

Sert deux rôles simultanément :
  ① Interface web de saisie manuelle d'activités Strava-like
     → démo live : saisie → Slack en < 10 secondes
  ② API REST appelée par les flows Kestra
     → Kestra (Docker) appelle http://host.docker.internal:5001/api/*

Routes UI :
  GET  /          → formulaire de saisie + historique récent
  GET  /health    → health check (utilisé par Kestra pour vérifier la dispo)

Routes API (appelées par Kestra) :
  POST /api/activity    → INSERT manuelle + notification Slack immédiate
  POST /api/ingest      → déclenche src/ingest_xlsx.py
  POST /api/generate    → déclenche src/generate_strava.py
  POST /api/etl         → déclenche src/etl_silver.py + src/etl_gold.py
  POST /api/quality     → déclenche src/quality_check.py
  POST /api/notify      → déclenche src/slack_notifier.py (poll)
  GET  /api/stats       → KPI Gold depuis PostgreSQL (pour la démo)

Usage:
    conda activate datascience2
    python src/flask_entry.py
    # → http://localhost:5001

    python src/flask_entry.py --port 5001 --debug
"""
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

from flask import Flask, jsonify, request, Response
import psycopg2.extras

from config import SPORT_PARAMS
from database import get_connection, log_run, ping

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/flask_entry.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["JSON_ENSURE_ASCII"] = False


# ════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════

def ok(data: Any = None, message: str = "OK") -> Response:
    return jsonify({"status": "ok", "message": message, "data": data})


def err(message: str, code: int = 400) -> tuple[Response, int]:
    return jsonify({"status": "error", "message": message}), code


def _employees_list() -> list[dict]:
    """Retourne la liste des salariés pour le formulaire."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT id, nom, prenom, bu, sport_declare FROM employees ORDER BY nom, prenom"
            )
            return [dict(r) for r in cur.fetchall()]


def _recent_activities(limit: int = 10) -> list[dict]:
    """Retourne les activités récentes pour l'affichage UI."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT sa.id, sa.employee_id, sa.sport_type,
                       sa.distance_m, sa.duree_s, sa.commentaire,
                       sa.source, sa.created_at,
                       e.nom, e.prenom
                FROM strava_activities sa
                LEFT JOIN employees e ON e.id = sa.employee_id
                ORDER BY sa.created_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()]


# ════════════════════════════════════════════════════════════════
# Route UI : formulaire de saisie
# ════════════════════════════════════════════════════════════════

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>POC Avantages Sportifs — Saisie Strava-like</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            background: #f1f5f9; color: #1e293b; padding: 32px 16px; }}
    .container {{ max-width: 900px; margin: 0 auto; }}
    h1 {{ font-size: 24px; font-weight: 700; color: #1e3a5c; margin-bottom: 4px; }}
    .subtitle {{ color: #64748b; font-size: 14px; margin-bottom: 32px; }}
    .card {{ background: white; border-radius: 12px; padding: 24px;
             box-shadow: 0 1px 8px rgba(0,0,0,.08); margin-bottom: 24px; }}
    .card h2 {{ font-size: 16px; font-weight: 600; color: #1e3a5c; margin-bottom: 20px;
                padding-bottom: 12px; border-bottom: 2px solid #0d9488; }}
    .form-row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }}
    label {{ display: block; font-size: 13px; font-weight: 500; color: #374151; margin-bottom: 6px; }}
    select, input, textarea {{
      width: 100%; padding: 10px 12px; border: 1px solid #d1d5db; border-radius: 8px;
      font-size: 14px; color: #1e293b; background: #f8fafc;
    }}
    select:focus, input:focus, textarea:focus {{
      outline: none; border-color: #0d9488; box-shadow: 0 0 0 3px rgba(13,148,136,.15);
    }}
    .btn {{ display: inline-flex; align-items: center; gap: 8px; padding: 12px 24px;
            border: none; border-radius: 8px; font-size: 15px; font-weight: 600;
            cursor: pointer; transition: all .15s; }}
    .btn-primary {{ background: #0d9488; color: white; }}
    .btn-primary:hover {{ background: #0f766e; }}
    .alert {{ padding: 12px 16px; border-radius: 8px; margin-bottom: 16px; font-size: 14px; display: none; }}
    .alert-success {{ background: #dcfce7; color: #166534; border: 1px solid #bbf7d0; }}
    .alert-error   {{ background: #fee2e2; color: #991b1b; border: 1px solid #fecaca; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th {{ background: #1e3a5c; color: white; padding: 10px 12px; text-align: left; }}
    td {{ padding: 10px 12px; border-bottom: 1px solid #f1f5f9; vertical-align: middle; }}
    tr:hover td {{ background: #f8fafc; }}
    .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px;
              font-weight: 600; background: #e0f2fe; color: #0369a1; }}
    .badge-manual {{ background: #dcfce7; color: #166534; }}
    .badge-gen {{ background: #fef9c3; color: #92400e; }}
    .api-card {{ background: #1e293b; color: #e2e8f0; border-radius: 12px; padding: 24px;
                 margin-bottom: 24px; }}
    .api-card h2 {{ color: #5eead4; border-color: #0d9488; }}
    code {{ font-family: "Consolas", monospace; font-size: 12px; color: #a5f3fc; }}
  </style>
</head>
<body>
<div class="container">

  <h1>🏃 POC Avantages Sportifs</h1>
  <p class="subtitle">Saisie manuelle d'activité Strava-like · Notification Slack en temps réel</p>

  <!-- Formulaire de saisie -->
  <div class="card">
    <h2>➕ Nouvelle activité sportive</h2>
    <div id="alert-success" class="alert alert-success">✅ Activité enregistrée et Slack notifié !</div>
    <div id="alert-error"   class="alert alert-error">❌ Erreur lors de l'enregistrement.</div>

    <form id="activity-form">
      <div class="form-row">
        <div>
          <label for="employee_id">Salarié</label>
          <select id="employee_id" name="employee_id" required>
            <option value="">— Sélectionner —</option>
            {employee_options}
          </select>
        </div>
        <div>
          <label for="sport_type">Sport</label>
          <select id="sport_type" name="sport_type" required>
            <option value="">— Sélectionner —</option>
            {sport_options}
          </select>
        </div>
      </div>
      <div class="form-row">
        <div>
          <label for="distance_km">Distance (km) — laisser vide si non pertinent</label>
          <input type="number" id="distance_km" name="distance_km"
                 min="0" max="500" step="0.1" placeholder="ex : 10.8">
        </div>
        <div>
          <label for="duree_min">Durée (minutes)</label>
          <input type="number" id="duree_min" name="duree_min"
                 min="1" max="1440" step="1" placeholder="ex : 46" required>
        </div>
      </div>
      <div style="margin-bottom: 20px">
        <label for="commentaire">Commentaire (optionnel)</label>
        <textarea id="commentaire" name="commentaire" rows="2"
                  placeholder="ex : Belle sortie matinale ! 🌅"></textarea>
      </div>
      <button type="submit" class="btn btn-primary">🚀 Enregistrer et notifier Slack</button>
    </form>
  </div>

  <!-- Historique -->
  <div class="card">
    <h2>📋 10 dernières activités</h2>
    <table>
      <thead>
        <tr>
          <th>ID</th><th>Salarié</th><th>Sport</th>
          <th>Distance</th><th>Durée</th><th>Commentaire</th><th>Source</th><th>Heure</th>
        </tr>
      </thead>
      <tbody id="activities-tbody">
        {activities_rows}
      </tbody>
    </table>
  </div>

  <!-- Endpoints API -->
  <div class="api-card">
    <div class="card" style="background:#1e293b;color:#e2e8f0;border:none">
      <h2 style="color:#5eead4;border-color:#0d9488">⚙️ API Kestra — Endpoints disponibles</h2>
      <p style="font-size:13px;color:#94a3b8;margin-bottom:16px">
        Kestra appelle ces endpoints via <code>http://host.docker.internal:5001/api/*</code>
      </p>
      <table style="color:#e2e8f0;font-size:13px">
        <tr><td style="padding:6px 12px;color:#5eead4"><code>POST /api/ingest</code></td>
            <td style="padding:6px 12px">Charge les XLSX dans raw_rh + raw_sport</td></tr>
        <tr><td style="padding:6px 12px;color:#5eead4"><code>POST /api/generate</code></td>
            <td style="padding:6px 12px">Lance le générateur Strava-like Monte Carlo</td></tr>
        <tr><td style="padding:6px 12px;color:#5eead4"><code>POST /api/etl</code></td>
            <td style="padding:6px 12px">ETL Silver + Gold (employees + avantages_calcules)</td></tr>
        <tr><td style="padding:6px 12px;color:#5eead4"><code>POST /api/quality</code></td>
            <td style="padding:6px 12px">Lance les 9 règles qualité</td></tr>
        <tr><td style="padding:6px 12px;color:#5eead4"><code>POST /api/notify</code></td>
            <td style="padding:6px 12px">Poll strava_activities → Slack (activités manuelles)</td></tr>
        <tr><td style="padding:6px 12px;color:#5eead4"><code>GET  /api/stats</code></td>
            <td style="padding:6px 12px">KPI Gold JSON (pour démo et monitoring)</td></tr>
        <tr><td style="padding:6px 12px;color:#5eead4"><code>GET  /health</code></td>
            <td style="padding:6px 12px">Health check utilisé par Kestra</td></tr>
      </table>
    </div>
  </div>

</div>

<script>
document.getElementById("activity-form").addEventListener("submit", async (e) => {{
  e.preventDefault();
  const alertOk  = document.getElementById("alert-success");
  const alertErr = document.getElementById("alert-error");
  alertOk.style.display = alertErr.style.display = "none";

  const body = {{
    employee_id:  document.getElementById("employee_id").value,
    sport_type:   document.getElementById("sport_type").value,
    distance_km:  document.getElementById("distance_km").value || null,
    duree_min:    parseInt(document.getElementById("duree_min").value),
    commentaire:  document.getElementById("commentaire").value || null,
  }};

  try {{
    const resp = await fetch("/api/activity", {{
      method: "POST",
      headers: {{"Content-Type": "application/json"}},
      body: JSON.stringify(body),
    }});
    const data = await resp.json();
    if (resp.ok) {{
      alertOk.style.display = "block";
      alertOk.textContent = "✅ Activité #" + data.data?.activity_id + " enregistrée ! Slack notifié 💬";
      e.target.reset();
      setTimeout(() => location.reload(), 2000);
    }} else {{
      alertErr.style.display = "block";
      alertErr.textContent = "❌ " + data.message;
    }}
  }} catch(err) {{
    alertErr.style.display = "block";
    alertErr.textContent = "❌ Erreur réseau : " + err.message;
  }}
}});
</script>
</body>
</html>"""


@app.route("/", methods=["GET"])
def index():
    """Interface de saisie manuelle."""
    from flask import render_template_string

    try:
        employees = _employees_list()
        activities = _recent_activities()
    except Exception as e:
        return f"<h1>Erreur DB</h1><p>{e}</p><p>Vérifiez que PostgreSQL est démarré.</p>", 500

    # Options salarié
    emp_opts = "\n".join(
        f'<option value="{e["id"]}">{e["prenom"]} {e["nom"]} ({e["bu"]})</option>'
        for e in employees
    )

    # Options sport
    sports = sorted(SPORT_PARAMS.keys())
    sport_opts = "\n".join(f'<option value="{s}">{s}</option>' for s in sports)

    # Lignes historique
    def _dur_str(s):
        if not s: return "—"
        m = int(s) // 60
        return f"{m}min" if m < 60 else f"{m//60}h{m%60:02d}"

    def _dist_str(d):
        if not d: return "—"
        return f"{float(d)/1000:.1f} km"

    def _src_badge(src):
        cls = "badge-manual" if src == "manual" else "badge-gen"
        return f'<span class="badge {cls}">{src}</span>'

    rows_html = ""
    for a in activities:
        rows_html += (
            f"<tr>"
            f"<td>{a['id']}</td>"
            f"<td>{a.get('prenom','?')} {a.get('nom','?')}</td>"
            f"<td>{a['sport_type']}</td>"
            f"<td>{_dist_str(a['distance_m'])}</td>"
            f"<td>{_dur_str(a['duree_s'])}</td>"
            f"<td>{a.get('commentaire') or '—'}</td>"
            f"<td>{_src_badge(a.get('source','?'))}</td>"
            f"<td>{str(a['created_at'])[:16] if a.get('created_at') else '—'}</td>"
            f"</tr>"
        )

    html = HTML_TEMPLATE.format(
        employee_options=emp_opts,
        sport_options=sport_opts,
        activities_rows=rows_html or "<tr><td colspan='8' style='text-align:center;color:#94a3b8'>Aucune activité</td></tr>",
    )
    return html


@app.route("/health", methods=["GET"])
def health():
    """Health check pour Kestra."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return ok({"postgres": "ok"}, "Service opérationnel")
    except Exception as e:
        return err(f"PostgreSQL KO : {e}", 503)


# ════════════════════════════════════════════════════════════════
# Routes API — saisie manuelle
# ════════════════════════════════════════════════════════════════

@app.route("/api/activity", methods=["POST"])
def api_activity():
    """
    Insère une activité manuelle et notifie Slack immédiatement.

    Body JSON :
      employee_id (str)     : identifiant du salarié
      sport_type  (str)     : type de sport
      duree_min   (int)     : durée en minutes
      distance_km (float?)  : distance en km (optionnel)
      commentaire (str?)    : commentaire libre (optionnel)

    Returns:
      JSON {status, data: {activity_id, slack_sent}}
    """
    body = request.get_json(force=True, silent=True) or {}

    employee_id = str(body.get("employee_id", "")).strip()
    sport_type  = str(body.get("sport_type",  "")).strip()
    duree_min   = body.get("duree_min")
    distance_km = body.get("distance_km")
    commentaire = body.get("commentaire")

    if not employee_id or not sport_type or not duree_min:
        return err("Champs requis : employee_id, sport_type, duree_min")

    # Convertir
    try:
        duree_s    = int(float(duree_min)) * 60
        distance_m = round(float(distance_km) * 1000, 1) if distance_km and float(distance_km) > 0 else None
    except (ValueError, TypeError) as e:
        return err(f"Valeur numérique invalide : {e}")

    # INSERT strava_activities
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO strava_activities
                    (employee_id, date_debut, date_fin, sport_type,
                     distance_m, duree_s, commentaire, source)
                VALUES (%s, NOW(), NOW() + INTERVAL '%s seconds', %s, %s, %s, %s, 'manual')
                RETURNING id
                """,
                (employee_id, duree_s, sport_type, distance_m, duree_s,
                 commentaire if commentaire else None),
            )
            activity_id = cur.fetchone()[0]

    logger.info(f"✅ Activité manuelle #{activity_id} : {sport_type} / {employee_id}")

    # Notifier Slack immédiatement
    from slack_notifier import notify_by_activity_id
    dry_run = os.getenv("SLACK_WEBHOOK_URL", "") == ""
    slack_ok = notify_by_activity_id(activity_id, dry_run=dry_run)

    log_run(
        flow_name="flask_entry",
        etape="manual_activity_insert",
        statut="SUCCESS",
        debut=datetime.now(),
        nb_lignes=1,
        metadata={"activity_id": activity_id, "sport": sport_type, "slack_sent": slack_ok},
    )

    return ok({"activity_id": activity_id, "slack_sent": slack_ok},
              f"Activité #{activity_id} créée")


# ════════════════════════════════════════════════════════════════
# Routes API — déclencheurs Kestra
# ════════════════════════════════════════════════════════════════

def _run_pipeline_step(step_name: str, fn, **kwargs) -> tuple[Response, int]:
    """Helper générique pour exécuter une étape et renvoyer le résultat."""
    debut = datetime.now()
    try:
        result = fn(**kwargs)
        log_run(flow_name=f"flask_api_{step_name}", etape=step_name,
                statut="SUCCESS", debut=debut,
                nb_lignes=result if isinstance(result, int) else 0)
        return ok({"result": result}, f"{step_name} OK")
    except SystemExit as e:
        log_run(flow_name=f"flask_api_{step_name}", etape=step_name,
                statut="FAILED", debut=debut, erreur=str(e))
        return err(f"{step_name} échoué : {e}", 500)
    except Exception as e:
        log_run(flow_name=f"flask_api_{step_name}", etape=step_name,
                statut="FAILED", debut=debut, erreur=str(e))
        logger.error(f"Erreur {step_name} : {e}", exc_info=True)
        return err(str(e), 500)


@app.route("/api/ingest", methods=["POST"])
def api_ingest():
    """Kestra flow 01 — ingestion XLSX."""
    from ingest_xlsx import main as fn
    body = request.get_json(force=True, silent=True) or {}
    return _run_pipeline_step("ingest_xlsx", fn,
                              table_filter=body.get("table"),
                              dry_run=body.get("dry_run", False))


@app.route("/api/generate", methods=["POST"])
def api_generate():
    """Kestra flow 02 — générateur Strava-like."""
    from generate_strava import main as fn
    body = request.get_json(force=True, silent=True) or {}
    return _run_pipeline_step("generate_strava", fn,
                              months=body.get("months", 12),
                              dry_run=body.get("dry_run", False))


@app.route("/api/etl", methods=["POST"])
def api_etl():
    """Kestra flow 03 — ETL Silver + Gold."""
    from etl_silver import main as silver
    from etl_gold   import main as gold
    body    = request.get_json(force=True, silent=True) or {}
    dry_run = body.get("dry_run", False)
    debut   = datetime.now()
    try:
        silver(dry_run=dry_run, skip_gmaps=body.get("skip_gmaps", False))
        metrics = gold(dry_run=dry_run)
        log_run(flow_name="flask_api_etl", etape="silver+gold",
                statut="SUCCESS", debut=debut, nb_lignes=metrics.get("nb_total", 0),
                metadata=metrics)
        return ok(metrics, "ETL Silver + Gold OK")
    except Exception as e:
        log_run(flow_name="flask_api_etl", etape="silver+gold",
                statut="FAILED", debut=debut, erreur=str(e))
        return err(str(e), 500)


@app.route("/api/quality", methods=["POST"])
def api_quality():
    """Kestra flow 04 — contrôles qualité."""
    from quality_check import main as fn
    body = request.get_json(force=True, silent=True) or {}
    debut = datetime.now()
    try:
        pipeline_ok = fn(suite=body.get("suite", "all"),
                         fail_fast=body.get("fail_fast", False))
        status = "SUCCESS" if pipeline_ok else "FAILED"
        log_run(flow_name="flask_api_quality", etape="quality_check",
                statut=status, debut=debut, nb_lignes=9)
        return ok({"pipeline_ok": pipeline_ok}, f"Quality check {status}")
    except Exception as e:
        return err(str(e), 500)


@app.route("/api/notify", methods=["POST"])
def api_notify():
    """Kestra flow 05 — notifications Slack (mode poll)."""
    from slack_notifier import poll_and_notify
    body     = request.get_json(force=True, silent=True) or {}
    dry_run  = body.get("dry_run", False)
    minutes  = body.get("minutes", 5)
    debut    = datetime.now()
    nb_sent  = poll_and_notify(since_minutes=minutes, dry_run=dry_run)
    log_run(flow_name="flask_api_notify", etape="poll_slack",
            statut="SUCCESS", debut=debut, nb_lignes=nb_sent,
            metadata={"since_minutes": minutes})
    return ok({"notifications_sent": nb_sent}, f"{nb_sent} notification(s) Slack envoyée(s)")


@app.route("/api/stats", methods=["GET"])
def api_stats():
    """KPI Gold en JSON — pour démo et monitoring Kestra."""
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT
                        COUNT(*)                                             AS total_salaries,
                        SUM(CASE WHEN eligible_prime    THEN 1 ELSE 0 END)  AS nb_prime,
                        ROUND(SUM(montant_prime)::NUMERIC, 2)               AS cout_primes,
                        SUM(CASE WHEN eligible_jours_be THEN 1 ELSE 0 END)  AS nb_be,
                        SUM(nb_jours_bienetre)                               AS total_jours,
                        MAX(params_version)                                  AS version
                    FROM avantages_calcules
                """)
                row = cur.fetchone()
                cur.execute("SELECT COUNT(*) FROM strava_activities")
                nb_acts = cur.fetchone()[0]
        data = dict(row) if row else {}
        data["nb_activites"] = nb_acts
        return ok(data, "KPI Gold")
    except Exception as e:
        return err(str(e), 500)


# ════════════════════════════════════════════════════════════════
# Import os manquant — ajout
# ════════════════════════════════════════════════════════════════
import os


# ════════════════════════════════════════════════════════════════
# Point d'entrée
# ════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Flask Entry — Round 4")
    parser.add_argument("--port",  type=int, default=5001, help="Port (défaut : 5001)")
    parser.add_argument("--debug", action="store_true",    help="Mode debug Flask")
    args = parser.parse_args()

    if not ping(max_retries=3, delay=1):
        logger.error("PostgreSQL non accessible — vérifiez Docker")
        sys.exit(1)

    logger.info(f"🌐 Flask démarré : http://localhost:{args.port}")
    logger.info(f"   API Kestra    : http://host.docker.internal:{args.port}/api/*")
    app.run(host="0.0.0.0", port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()

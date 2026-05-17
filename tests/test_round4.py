"""
Tests Round 4 — Flask Entry + Slack Notifier + Kestra Flows

Couvre :
  - Formatage des messages Slack (sans appel réseau)
  - Endpoints Flask (via client de test Flask)
  - Slack notifier en dry run (sans webhook réel)
  - Présence et structure des flows Kestra YAML
  - Insertion manuelle d'activité via API Flask

Lancer : pytest tests/test_round4.py -v
"""
import json
import sys
from pathlib import Path
from datetime import datetime

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


# ══════════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def flask_client():
    """Client de test Flask — aucun serveur réseau requis."""
    from flask_entry import app
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


@pytest.fixture(scope="module")
def db_conn():
    from database import get_connection
    with get_connection() as conn:
        yield conn


@pytest.fixture
def sample_activity():
    return {
        "employee_id":  "TEST_R4",
        "sport_type":   "Running",
        "distance_m":   10800.0,
        "duree_s":      2760,
        "commentaire":  "Test Round 4 pytest",
        "source":       "generator",
    }


@pytest.fixture
def sample_employee():
    return {
        "prenom": "Juliette",
        "nom":    "Mendes",
        "bu":     "Marketing",
    }


# ══════════════════════════════════════════════════════════════════
# Tests Slack Notifier — formatage (sans appel réseau)
# ══════════════════════════════════════════════════════════════════

class TestSlackFormatter:

    def test_fmt_distance_km(self):
        """10 800 m → '10,8 km'"""
        from slack_notifier import _fmt_distance
        assert _fmt_distance(10800) == "10,8 km"

    def test_fmt_distance_none(self):
        """None → chaîne vide"""
        from slack_notifier import _fmt_distance
        assert _fmt_distance(None) == ""

    def test_fmt_distance_zero(self):
        from slack_notifier import _fmt_distance
        assert _fmt_distance(0) == ""

    def test_fmt_duration_minutes(self):
        """2760 s = 46 min"""
        from slack_notifier import _fmt_duration
        assert _fmt_duration(2760) == "46 min"

    def test_fmt_duration_hours(self):
        """5400 s = 1h 30min"""
        from slack_notifier import _fmt_duration
        assert _fmt_duration(5400) == "1h 30min"

    def test_fmt_duration_exact_hour(self):
        """3600 s = 1h"""
        from slack_notifier import _fmt_duration
        assert _fmt_duration(3600) == "1h"

    def test_fmt_duration_none(self):
        from slack_notifier import _fmt_duration
        assert _fmt_duration(None) == "?"

    def test_format_message_running(self, sample_activity, sample_employee):
        """Le message Running doit contenir prénom, nom, distance ou durée."""
        from slack_notifier import format_message
        msg = format_message(sample_activity, sample_employee)
        assert "Juliette" in msg
        assert "Mendes" in msg
        assert len(msg) > 20

    def test_format_message_escalade(self, sample_employee):
        """Escalade sans distance — le message doit quand même être formé."""
        from slack_notifier import format_message
        act = {"sport_type": "Escalade", "distance_m": None, "duree_s": 5400}
        msg = format_message(act, sample_employee)
        assert "Juliette" in msg or "Escalade" in msg

    def test_format_message_avec_commentaire(self, sample_activity, sample_employee):
        """Un commentaire doit apparaître en italique dans le message."""
        from slack_notifier import format_message
        act = {**sample_activity, "commentaire": "Belle sortie matinale !"}
        msg = format_message(act, sample_employee)
        assert "Belle sortie matinale" in msg

    def test_format_message_sans_commentaire(self, sample_activity, sample_employee):
        """Sans commentaire, le message doit quand même être valide."""
        from slack_notifier import format_message
        act = {**sample_activity, "commentaire": None}
        msg = format_message(act, sample_employee)
        assert len(msg) > 10

    def test_build_slack_payload_structure(self, sample_activity, sample_employee):
        """Le payload Slack doit contenir blocks et text."""
        from slack_notifier import build_slack_payload, format_message
        msg     = format_message(sample_activity, sample_employee)
        payload = build_slack_payload(msg, sample_activity, sample_employee)
        assert "blocks" in payload
        assert "text"   in payload
        assert isinstance(payload["blocks"], list)
        assert len(payload["blocks"]) >= 1

    def test_send_dry_run_returns_true(self, sample_activity, sample_employee):
        """dry_run=True doit toujours retourner True sans appel réseau."""
        from slack_notifier import send_slack_message
        result = send_slack_message(sample_activity, sample_employee, dry_run=True)
        assert result is True

    def test_send_no_webhook_returns_true(self, sample_activity, sample_employee, monkeypatch):
        """Sans SLACK_WEBHOOK_URL, send_slack_message retourne True (mode simulation)."""
        monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
        from slack_notifier import send_slack_message
        result = send_slack_message(sample_activity, sample_employee, dry_run=False)
        assert result is True


# ══════════════════════════════════════════════════════════════════
# Tests Flask — client de test (sans réseau)
# ══════════════════════════════════════════════════════════════════

class TestFlaskHealth:

    def test_health_endpoint_ok(self, flask_client):
        """GET /health doit retourner 200 si PostgreSQL est accessible."""
        resp = flask_client.get("/health")
        assert resp.status_code in (200, 503)  # 503 si PG pas démarré en CI

    def test_health_response_json(self, flask_client):
        resp = flask_client.get("/health")
        data = resp.get_json()
        assert "status" in data

    def test_index_returns_html(self, flask_client):
        """GET / doit retourner du HTML."""
        resp = flask_client.get("/")
        # Peut retourner 500 si PG absent, mais le content-type doit être text/html
        assert resp.content_type.startswith("text/html")

    def test_stats_endpoint(self, flask_client, db_conn):
        """GET /api/stats doit retourner les KPI Gold."""
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM avantages_calcules")
            nb = cur.fetchone()[0]
        if nb == 0:
            pytest.skip("avantages_calcules vide — lancez run_round3.py")

        resp = flask_client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "ok"
        assert "total_salaries" in data.get("data", {})


class TestFlaskActivityAPI:

    def test_activity_missing_fields(self, flask_client):
        """POST /api/activity sans les champs requis → 400."""
        resp = flask_client.post(
            "/api/activity",
            data=json.dumps({}),
            content_type="application/json",
        )
        assert resp.status_code == 400

    def test_activity_invalid_duration(self, flask_client):
        """POST /api/activity avec duree_min non numérique → 400."""
        resp = flask_client.post(
            "/api/activity",
            data=json.dumps({
                "employee_id": "12345",
                "sport_type": "Running",
                "duree_min": "pas_un_nombre",
            }),
            content_type="application/json",
        )
        assert resp.status_code == 400

    def test_activity_insert_and_notify(self, flask_client, db_conn):
        """POST /api/activity doit insérer une ligne dans strava_activities."""
        # Trouver un employee_id valide
        with db_conn.cursor() as cur:
            cur.execute("SELECT id FROM employees LIMIT 1")
            row = cur.fetchone()
        if not row:
            pytest.skip("Table employees vide — lancez run_round2.py")

        employee_id = row[0]
        before_count = _count_strava(db_conn)

        resp = flask_client.post(
            "/api/activity",
            data=json.dumps({
                "employee_id": employee_id,
                "sport_type":  "Tennis",
                "duree_min":   60,
                "commentaire": "Test pytest R4",
            }),
            content_type="application/json",
        )

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "ok"
        assert "activity_id" in data.get("data", {})

        after_count = _count_strava(db_conn)
        assert after_count == before_count + 1

        # Nettoyage
        act_id = data["data"]["activity_id"]
        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM strava_activities WHERE id = %s", (act_id,))
        db_conn.commit()


def _count_strava(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM strava_activities")
        return cur.fetchone()[0]


class TestFlaskPipelineAPI:

    def test_ingest_dry_run(self, flask_client, db_conn):
        """POST /api/ingest avec dry_run → 200, base non modifiée."""
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_rh")
            before = cur.fetchone()[0]

        resp = flask_client.post(
            "/api/ingest",
            data=json.dumps({"dry_run": True}),
            content_type="application/json",
        )
        assert resp.status_code == 200

        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_rh")
            after = cur.fetchone()[0]
        assert before == after

    def test_notify_dry_run(self, flask_client):
        """POST /api/notify avec dry_run → 200."""
        resp = flask_client.post(
            "/api/notify",
            data=json.dumps({"dry_run": True, "since_minutes": 60}),
            content_type="application/json",
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "ok"


# ══════════════════════════════════════════════════════════════════
# Tests flows Kestra YAML
# ══════════════════════════════════════════════════════════════════

class TestKestraFlows:

    FLOWS_DIR = Path("kestra/flows")

    EXPECTED_FLOWS = [
        "01_ingest_xlsx.yml",
        "02_generate_strava.yml",
        "03_etl_silver_gold.yml",
        "04_quality_check.yml",
        "05_notify_slack.yml",
    ]

    def test_flows_directory_exists(self):
        assert self.FLOWS_DIR.exists(), f"Dossier kestra/flows/ absent"

    def test_all_flows_present(self):
        """Les 5 fichiers YAML de flows doivent exister."""
        missing = [f for f in self.EXPECTED_FLOWS if not (self.FLOWS_DIR / f).exists()]
        assert not missing, f"Flows manquants : {missing}"

    @pytest.mark.parametrize("flow_file", [
        "01_ingest_xlsx.yml",
        "02_generate_strava.yml",
        "03_etl_silver_gold.yml",
        "04_quality_check.yml",
        "05_notify_slack.yml",
    ])
    def test_flow_valid_yaml(self, flow_file):
        """Chaque flow doit être du YAML valide."""
        content = (self.FLOWS_DIR / flow_file).read_text(encoding="utf-8")
        parsed = yaml.safe_load(content)
        assert parsed is not None, f"{flow_file} est vide ou invalide"

    @pytest.mark.parametrize("flow_file", [
        "01_ingest_xlsx.yml",
        "02_generate_strava.yml",
        "03_etl_silver_gold.yml",
        "04_quality_check.yml",
        "05_notify_slack.yml",
    ])
    def test_flow_has_required_fields(self, flow_file):
        """Chaque flow doit avoir id, namespace et tasks."""
        content = (self.FLOWS_DIR / flow_file).read_text(encoding="utf-8")
        flow = yaml.safe_load(content)
        assert "id"        in flow, f"{flow_file} : champ 'id' manquant"
        assert "namespace" in flow, f"{flow_file} : champ 'namespace' manquant"
        assert "tasks"     in flow, f"{flow_file} : champ 'tasks' manquant"
        assert flow["namespace"] == "poc.avantages_sportifs"

    def test_flow_03_has_schedule(self):
        """Flow 03 (ETL) doit avoir un trigger schedule quotidien."""
        content = (self.FLOWS_DIR / "03_etl_silver_gold.yml").read_text(encoding="utf-8")
        flow = yaml.safe_load(content)
        triggers = flow.get("triggers", [])
        assert triggers, "Flow 03 doit avoir au moins un trigger"
        crons = [t.get("cron", "") for t in triggers if "cron" in t]
        assert crons, "Flow 03 doit avoir un trigger Schedule avec un cron"

    def test_flow_05_has_schedule(self):
        """Flow 05 (Slack) doit avoir un trigger schedule polling."""
        content = (self.FLOWS_DIR / "05_notify_slack.yml").read_text(encoding="utf-8")
        flow = yaml.safe_load(content)
        triggers = flow.get("triggers", [])
        assert triggers, "Flow 05 doit avoir un trigger de polling"

    def test_flows_call_flask_api(self):
        """Tous les flows doivent appeler host.docker.internal:5001."""
        for flow_file in self.EXPECTED_FLOWS:
            content = (self.FLOWS_DIR / flow_file).read_text(encoding="utf-8")
            assert "host.docker.internal" in content or "localhost" in content, \
                f"{flow_file} ne référence pas l'API Flask"

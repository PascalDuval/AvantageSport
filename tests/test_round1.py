"""
Tests Round 1 — Infra + Générateur Strava-like

Couvre :
  - Connexion PostgreSQL
  - Présence des 9 tables
  - Génération d'activités (sans insertion)
  - Cohérence des données générées
  - Delta Lake Bronze lisible par DuckDB

Lancer : pytest tests/test_round1.py -v
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


# ══════════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def db_conn():
    """Connexion PostgreSQL partagée pour le module de tests."""
    from database import get_connection
    with get_connection() as conn:
        yield conn


# ══════════════════════════════════════════════════════════════════
# Tests PostgreSQL
# ══════════════════════════════════════════════════════════════════

class TestPostgreSQL:

    def test_connexion(self):
        """PostgreSQL doit être accessible en localhost:5432."""
        from database import ping
        assert ping(max_retries=3, delay=1), "PostgreSQL non accessible"

    def test_tables_presentes(self, db_conn):
        """Les 9 tables du schéma doivent exister."""
        tables_attendues = {
            "raw_rh", "raw_sport", "strava_activities",
            "employees", "avantages_calcules", "config",
            "gmaps_cache", "pipeline_runs", "data_quality_results"
        }
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
            """)
            tables_presentes = {row[0] for row in cur.fetchall()}

        manquantes = tables_attendues - tables_presentes
        assert not manquantes, f"Tables manquantes : {manquantes}"

    def test_config_peuplee(self, db_conn):
        """La table config doit contenir les paramètres par défaut."""
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM config")
            nb = cur.fetchone()[0]
        assert nb >= 8, f"Table config : {nb} lignes (minimum 8 attendues)"

    def test_config_taux_prime(self, db_conn):
        """Le taux de prime doit être à 0.05 par défaut."""
        with db_conn.cursor() as cur:
            cur.execute("SELECT valeur FROM config WHERE cle = 'taux_prime'")
            row = cur.fetchone()
        assert row is not None, "Clé taux_prime absente de config"
        assert float(row[0]) == 0.05, f"taux_prime attendu 0.05, obtenu {row[0]}"


# ══════════════════════════════════════════════════════════════════
# Tests générateur Strava
# ══════════════════════════════════════════════════════════════════

class TestGenerator:

    def test_generate_activity_running(self):
        """Une activité Running doit avoir distance et durée cohérentes."""
        from generate_strava import generate_activity
        act = generate_activity("99999", "Running", datetime.now() - timedelta(days=30))
        assert act["sport_type"] == "Running"
        assert act["distance_m"] is not None
        assert 3000 <= act["distance_m"] <= 18000, f"Distance hors limites : {act['distance_m']}"
        assert 1080 <= act["duree_s"] <= 5400, f"Durée hors limites : {act['duree_s']}"
        assert act["date_fin"] > act["date_debut"]
        assert act["source"] == "generator"

    def test_generate_activity_escalade(self):
        """Escalade : pas de distance (None attendu)."""
        from generate_strava import generate_activity
        act = generate_activity("99999", "Escalade", datetime.now() - timedelta(days=10))
        assert act["distance_m"] is None, "Escalade ne doit pas avoir de distance"
        assert act["duree_s"] > 0

    def test_generate_activity_runing_typo(self):
        """'Runing' (typo) doit être accepté sans erreur."""
        from generate_strava import generate_activity
        act = generate_activity("99999", "Runing", datetime.now() - timedelta(days=5))
        assert act["sport_type"] == "Runing"
        assert act["distance_m"] is not None  # Traité comme Running

    def test_generate_activities_for_athlete(self):
        """Un athlète sur 12 mois doit avoir entre 0 et 60 activités (0-5/mois)."""
        from generate_strava import generate_activities_for_athlete, ACTS_MAX_PER_MONTH
        acts = generate_activities_for_athlete("12345", "Tennis")
        assert 0 <= len(acts) <= 12 * ACTS_MAX_PER_MONTH, \
            f"Nombre d'activités hors limites : {len(acts)}"
        assert all(a["date_debut"].year == 2025 for a in acts), \
            "Des activités sont hors de l'année 2025"

    def test_no_future_activities(self):
        """Toutes les activités sont en 2025 — donc dans le passé."""
        from generate_strava import generate_activities_for_athlete
        now = datetime.now()
        acts = generate_activities_for_athlete("11111", "Natation")
        future = [a for a in acts if a["date_debut"] > now]
        assert not future, f"{len(future)} activités dans le futur (année 2025 devrait être passée)"

    def test_load_athletes_file_not_found(self, tmp_path, monkeypatch):
        """Une erreur claire doit être levée si les XLSX sont absents."""
        import config as cfg
        monkeypatch.setattr(cfg, "RH_FILE", tmp_path / "inexistant.xlsx")
        from generate_strava import load_athletes
        with pytest.raises(FileNotFoundError):
            load_athletes()


# ══════════════════════════════════════════════════════════════════
# Tests insertion (nécessite PostgreSQL)
# ══════════════════════════════════════════════════════════════════

class TestInsertion:

    def test_insert_activities_dry_run(self):
        """dry_run doit retourner 0 sans insérer."""
        from generate_strava import generate_activity, insert_activities
        acts = [generate_activity("00001", "Tennis", datetime.now() - timedelta(days=1))]
        nb = insert_activities(acts, dry_run=True)
        assert nb == 0

    def test_insert_activities_real(self, db_conn):
        """Insérer 1 activité test et vérifier qu'elle est en base."""
        from generate_strava import insert_activities
        test_date = datetime(2024, 1, 15, 8, 0, 0)
        acts = [{
            "employee_id": "TEST_PYTEST",
            "date_debut": test_date,
            "date_fin": test_date + timedelta(seconds=3600),
            "sport_type": "Running",
            "distance_m": 10000,
            "duree_s": 3600,
            "commentaire": "Test pytest Round 1",
            "source": "test"
        }]
        nb = insert_activities(acts)
        assert nb == 1

        # Vérifier en base
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT distance_m FROM strava_activities WHERE employee_id = 'TEST_PYTEST'"
            )
            row = cur.fetchone()
        assert row is not None, "Activité test non trouvée en base"
        assert row[0] == 10000

        # Nettoyage
        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM strava_activities WHERE employee_id = 'TEST_PYTEST'")
        db_conn.commit()


# ══════════════════════════════════════════════════════════════════
# Tests Delta Lake Bronze
# ══════════════════════════════════════════════════════════════════

class TestBronze:

    def test_bronze_strava_exists(self):
        """Le dossier Delta Lake Bronze strava doit exister après l'écriture."""
        from config import BRONZE_DIR
        bronze_path = BRONZE_DIR / "strava"
        if not bronze_path.exists():
            pytest.skip("Bronze pas encore écrit — lancez run_round1.py d'abord")

        assert (bronze_path / "_delta_log").exists(), "_delta_log absent"
        parquets = list(bronze_path.glob("**/*.parquet"))
        assert len(parquets) > 0, "Aucun fichier Parquet dans Bronze"

    def test_bronze_readable_by_duckdb(self):
        """DuckDB doit pouvoir lire le Delta Lake Bronze."""
        from config import BRONZE_DIR
        import duckdb

        bronze_path = str(BRONZE_DIR / "strava")
        if not (BRONZE_DIR / "strava").exists():
            pytest.skip("Bronze pas encore écrit")

        conn = duckdb.connect()
        conn.execute("INSTALL delta; LOAD delta;")
        df = conn.execute(f"SELECT COUNT(*) AS n FROM delta_scan('{bronze_path}')").fetchdf()
        conn.close()

        assert df["n"].iloc[0] > 0, "Delta Lake Bronze vide"

    def test_bronze_schema_correct(self):
        """Les colonnes attendues doivent être présentes dans le Bronze."""
        from config import BRONZE_DIR
        import duckdb

        bronze_path = str(BRONZE_DIR / "strava")
        if not (BRONZE_DIR / "strava").exists():
            pytest.skip("Bronze pas encore écrit")

        conn = duckdb.connect()
        conn.execute("INSTALL delta; LOAD delta;")
        df = conn.execute(f"SELECT * FROM delta_scan('{bronze_path}') LIMIT 1").fetchdf()
        conn.close()

        colonnes_attendues = {"employee_id", "date_debut", "sport_type", "distance_m", "duree_s"}
        assert colonnes_attendues.issubset(set(df.columns)), \
            f"Colonnes manquantes : {colonnes_attendues - set(df.columns)}"

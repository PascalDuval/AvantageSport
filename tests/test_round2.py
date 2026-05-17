"""
Tests Round 2 — Ingestion XLSX + ETL Silver + Google Maps

Couvre :
  - Fonctions de normalisation (IDs, dates, modes, sports)
  - Ingestion XLSX dry-run
  - Google Maps mock (aucun appel réseau)
  - Construction du DataFrame Silver
  - Table employees peuplée et cohérente

Lancer : pytest tests/test_round2.py -v
         pytest tests/test_round2.py -v --skip-gmaps  (via marker)
"""
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


# ══════════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def db_conn():
    from database import get_connection
    with get_connection() as conn:
        yield conn


# ══════════════════════════════════════════════════════════════════
# Tests de normalisation
# ══════════════════════════════════════════════════════════════════

class TestNormalisation:
    """Valide chaque fonction de normalisation de etl_silver.py."""

    def test_normalize_id_float(self):
        """'59019.0' → '59019'"""
        from etl_silver import normalize_id
        assert normalize_id("59019.0") == "59019"

    def test_normalize_id_int(self):
        """'59019' → '59019'"""
        from etl_silver import normalize_id
        assert normalize_id("59019") == "59019"

    def test_normalize_id_none(self):
        """None → None"""
        from etl_silver import normalize_id
        assert normalize_id(None) is None
        assert normalize_id("nan") is None
        assert normalize_id("") is None

    def test_normalize_date_excel_serial(self):
        """'33060.0' doit donner une date cohérente (années 1990)."""
        from etl_silver import normalize_date
        d = normalize_date("33060.0")
        assert d is not None
        assert isinstance(d, date)
        assert 1980 <= d.year <= 2000

    def test_normalize_date_iso(self):
        """'1990-07-06 00:00:00' → date(1990, 7, 6)"""
        from etl_silver import normalize_date
        d = normalize_date("1990-07-06 00:00:00")
        assert d == date(1990, 7, 6)

    def test_normalize_date_none(self):
        from etl_silver import normalize_date
        assert normalize_date(None) is None
        assert normalize_date("nan") is None

    def test_normalize_salaire(self):
        """'30940.0' → 30940.00"""
        from etl_silver import normalize_salaire
        assert normalize_salaire("30940.0") == 30940.0
        assert normalize_salaire(None) is None

    def test_normalize_mode_marche(self):
        """'Marche/running' → 'marche_running'"""
        from etl_silver import normalize_mode_deplacement
        assert normalize_mode_deplacement("Marche/running") == "marche_running"

    def test_normalize_mode_velo(self):
        """'Vélo/Trottinette/Autres' → 'velo_trottinette'"""
        from etl_silver import normalize_mode_deplacement
        assert normalize_mode_deplacement("Vélo/Trottinette/Autres") == "velo_trottinette"

    def test_normalize_mode_tc(self):
        from etl_silver import normalize_mode_deplacement
        assert normalize_mode_deplacement("Transports en commun") == "transports_commun"

    def test_normalize_mode_vehicule(self):
        from etl_silver import normalize_mode_deplacement
        assert normalize_mode_deplacement("véhicule thermique/électrique") == "vehicule"

    def test_normalize_mode_none(self):
        from etl_silver import normalize_mode_deplacement
        assert normalize_mode_deplacement(None) is None
        assert normalize_mode_deplacement("") is None

    def test_normalize_sport_typo_runing(self):
        """'Runing' → 'Running' (correction de la typo clé)"""
        from etl_silver import normalize_sport
        assert normalize_sport("Runing") == "Running"

    def test_normalize_sport_running_correct(self):
        from etl_silver import normalize_sport
        assert normalize_sport("Running") == "Running"

    def test_normalize_sport_randonnee(self):
        from etl_silver import normalize_sport
        assert normalize_sport("Randonnée") == "Randonnée"

    def test_normalize_sport_none(self):
        from etl_silver import normalize_sport
        assert normalize_sport(None) is None
        assert normalize_sport("nan") is None


# ══════════════════════════════════════════════════════════════════
# Tests logique métier
# ══════════════════════════════════════════════════════════════════

class TestEligibilite:
    """Valide la logique d'éligibilité à la prime."""

    def test_marche_sous_seuil(self):
        from etl_silver import compute_eligible_prime
        assert compute_eligible_prime("marche_running", 10.0, 15.0, 25.0) is True

    def test_marche_sur_seuil(self):
        from etl_silver import compute_eligible_prime
        assert compute_eligible_prime("marche_running", 16.0, 15.0, 25.0) is False

    def test_velo_sous_seuil(self):
        from etl_silver import compute_eligible_prime
        assert compute_eligible_prime("velo_trottinette", 20.0, 15.0, 25.0) is True

    def test_velo_sur_seuil(self):
        from etl_silver import compute_eligible_prime
        assert compute_eligible_prime("velo_trottinette", 26.0, 15.0, 25.0) is False

    def test_tc_jamais_eligible(self):
        """TC non éligible même avec distance dans les limites."""
        from etl_silver import compute_eligible_prime
        assert compute_eligible_prime("transports_commun", 5.0, 15.0, 25.0) is False

    def test_vehicule_jamais_eligible(self):
        """Véhicule non éligible même avec distance dans les limites."""
        from etl_silver import compute_eligible_prime
        assert compute_eligible_prime("vehicule", 3.0, 15.0, 25.0) is False

    def test_tc_distance_none(self):
        """TC avec distance=None (cas réel pipeline) → non éligible."""
        from etl_silver import compute_eligible_prime
        assert compute_eligible_prime("transports_commun", None, 15.0, 25.0) is False

    def test_vehicule_distance_none(self):
        """Véhicule avec distance=None (cas réel pipeline) → non éligible."""
        from etl_silver import compute_eligible_prime
        assert compute_eligible_prime("vehicule", None, 15.0, 25.0) is False

    def test_distance_none(self):
        from etl_silver import compute_eligible_prime
        assert compute_eligible_prime("marche_running", None, 15.0, 25.0) is False

    def test_mode_none(self):
        from etl_silver import compute_eligible_prime
        assert compute_eligible_prime(None, 5.0, 15.0, 25.0) is False

    def test_exactement_au_seuil_marche(self):
        """Exactement 15 km → éligible (≤)."""
        from etl_silver import compute_eligible_prime
        assert compute_eligible_prime("marche_running", 15.0, 15.0, 25.0) is True


# ══════════════════════════════════════════════════════════════════
# Tests Google Maps mock
# ══════════════════════════════════════════════════════════════════

class TestGmapsMock:
    """Valide le client Google Maps en mode mock (aucun appel réseau)."""

    @pytest.fixture
    def mock_client(self):
        from gmaps_client import GmapsClient
        return GmapsClient(api_key=None, mock_mode=True)

    def test_mock_distance_not_none(self, mock_client):
        """Le mock doit toujours retourner une distance."""
        d = mock_client.distance_to_bureau("3 rue de la Paix, Montpellier", "marche_running")
        assert d is not None
        assert isinstance(d, float)
        assert d > 0

    def test_mock_distance_deterministe(self, mock_client):
        """La même adresse doit toujours donner la même distance."""
        addr = "15 avenue de la Liberté, 34000 Montpellier"
        d1 = mock_client.distance_to_bureau(addr, "velo_trottinette")
        d2 = mock_client.distance_to_bureau(addr, "velo_trottinette")
        assert d1 == d2

    def test_mock_distances_differentes(self, mock_client):
        """Deux adresses différentes doivent donner des distances différentes."""
        d1 = mock_client.distance_to_bureau("1 rue Alpha, Montpellier", "marche_running")
        d2 = mock_client.distance_to_bureau("99 boulevard Omega, Nîmes", "marche_running")
        assert d1 != d2

    def test_batch_distances_skips_non_eligible_modes(self, mock_client, monkeypatch):
        """batch_distances ne doit recevoir que des employés en mode éligible."""
        captured = []
        original = mock_client.distance_to_bureau

        def tracking_distance(addr, mode):
            captured.append(mode)
            return original(addr, mode)

        monkeypatch.setattr(mock_client, "distance_to_bureau", tracking_distance)

        employees = [
            {"id": "1", "adresse_domicile": "1 rue A, Montpellier", "mode_norm": "marche_running"},
            {"id": "2", "adresse_domicile": "2 rue B, Montpellier", "mode_norm": "transports_commun"},
            {"id": "3", "adresse_domicile": "3 rue C, Montpellier", "mode_norm": "vehicule"},
            {"id": "4", "adresse_domicile": "4 rue D, Montpellier", "mode_norm": "velo_trottinette"},
        ]
        # Simuler le filtrage de build_silver_df
        MODES_ELIGIBLES = {"marche_running", "velo_trottinette"}
        eligible = [e for e in employees if e["mode_norm"] in MODES_ELIGIBLES]
        mock_client.batch_distances(eligible, mode_col="mode_norm", addr_col="adresse_domicile")

        assert set(captured) <= MODES_ELIGIBLES, (
            f"Modes non éligibles appelés : {set(captured) - MODES_ELIGIBLES}"
        )
        assert len(captured) == 2  # seulement marche + vélo

    def test_mock_geocode(self, mock_client):
        """Le géocodage mock doit retourner des coordonnées cohérentes."""
        lat, lng = mock_client.geocode("5 rue Pasteur, 34000 Montpellier")
        assert lat is not None and lng is not None
        assert 43.0 <= lat <= 44.5  # Région Montpellier
        assert 3.0 <= lng <= 5.0

    def test_adresse_vide(self, mock_client):
        """Une adresse vide doit retourner None."""
        d = mock_client.distance_to_bureau("", "marche_running")
        assert d is None

    def test_get_client_sans_api_key(self, monkeypatch):
        """Sans clé API, get_client() doit retourner un client mock."""
        monkeypatch.delenv("GOOGLE_MAPS_API_KEY", raising=False)
        monkeypatch.setenv("GMAPS_MOCK_MODE", "true")
        from gmaps_client import get_client
        client = get_client()
        assert client.mock_mode is True


# ══════════════════════════════════════════════════════════════════
# Tests ingestion (dry-run, pas d'effet en base)
# ══════════════════════════════════════════════════════════════════

class TestIngestDryRun:

    def test_ingest_dry_run_no_insert(self, db_conn):
        """Le dry-run ne doit pas modifier le nombre de lignes en base."""
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_rh")
            before = cur.fetchone()[0]

        from ingest_xlsx import main as ingest
        result = ingest(dry_run=True)

        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_rh")
            after = cur.fetchone()[0]

        assert before == after, "dry_run a quand même inséré des données"
        assert result["nb_rh"] == 0
        assert result["nb_sport"] == 0

    def test_read_rh_structure(self):
        """Le fichier RH doit pouvoir être lu et contenir les colonnes attendues."""
        from config import RH_FILE
        if not RH_FILE.exists():
            pytest.skip("Fichier RH absent de data/raw/")
        from ingest_xlsx import read_rh_xlsx
        df = read_rh_xlsx()
        assert len(df) > 0
        # Vérifier qu'au moins un identifiant ressemble à un ID salarié
        assert "ID salarié" in df.columns or "id_salarie" in df.columns

    def test_read_sport_structure(self):
        """Le fichier Sport doit avoir la colonne de pratique sportive."""
        from config import SPORT_FILE
        if not SPORT_FILE.exists():
            pytest.skip("Fichier Sport absent de data/raw/")
        from ingest_xlsx import read_sport_xlsx
        df = read_sport_xlsx()
        assert len(df) > 0
        assert "Pratique d'un sport" in df.columns


# ══════════════════════════════════════════════════════════════════
# Tests Silver (nécessite PostgreSQL + raw_rh peuplé)
# ══════════════════════════════════════════════════════════════════

class TestSilverDB:

    def test_employees_table_exists(self, db_conn):
        """La table employees doit exister."""
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = 'employees'
            """)
            assert cur.fetchone()[0] == 1

    def test_employees_peuplee_apres_etl(self, db_conn):
        """Après l'ETL Silver, employees doit contenir des données."""
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM employees")
            nb = cur.fetchone()[0]
        if nb == 0:
            pytest.skip("ETL Silver non encore lancé — lancez run_round2.py")
        assert nb > 0

    def test_employees_eligible_prime_coherent(self, db_conn):
        """Les éligibles prime doivent être en mode marche ou vélo."""
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM employees
                WHERE eligible_prime = TRUE
                AND mode_deplacement NOT IN ('marche_running', 'velo_trottinette')
            """)
            nb_incoherent = cur.fetchone()[0]
        assert nb_incoherent == 0, (
            f"{nb_incoherent} salariés éligibles prime avec un mode non sportif"
        )

    def test_sport_runing_corrige(self, db_conn):
        """Aucun 'Runing' (typo) ne doit subsister dans employees."""
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM employees WHERE sport_declare = 'Runing'
            """)
            nb = cur.fetchone()[0]
        assert nb == 0, f"{nb} entrées avec la typo 'Runing' dans employees"

    def test_ids_sans_decimal(self, db_conn):
        """Aucun ID avec '.0' ne doit subsister dans employees."""
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM employees WHERE id LIKE '%.%'")
            nb = cur.fetchone()[0]
        assert nb == 0, f"{nb} IDs avec décimale dans employees"

    def test_non_eligible_modes_null_distance(self, db_conn):
        """Les employés TC/véhicule ne doivent pas avoir de distance calculée."""
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM employees
                WHERE mode_deplacement IN ('transports_commun', 'vehicule')
                  AND distance_bureau_km IS NOT NULL
            """)
            nb = cur.fetchone()[0]
        assert nb == 0, (
            f"{nb} employés TC/véhicule ont une distance calculée (inattendu)"
        )

    def test_eligible_modes_have_distance(self, db_conn):
        """Les employés marche/vélo doivent avoir une distance calculée."""
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM employees
                WHERE mode_deplacement IN ('marche_running', 'velo_trottinette')
                  AND distance_bureau_km IS NULL
            """)
            nb = cur.fetchone()[0]
        assert nb == 0, (
            f"{nb} employés marche/vélo sans distance calculée"
        )

    def test_gmaps_cache_peuple(self, db_conn):
        """Le cache Google Maps doit contenir des entrées après l'ETL."""
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM gmaps_cache")
            nb = cur.fetchone()[0]
        if nb == 0:
            pytest.skip("gmaps_cache vide — ETL Silver non encore lancé")
        assert nb > 0

    def test_gmaps_cache_only_eligible_modes(self, db_conn):
        """Le cache ne doit contenir que des modes walking/bicycling (pas driving/transit)."""
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM gmaps_cache
                WHERE mode_transport IN ('driving', 'transit')
            """)
            nb = cur.fetchone()[0]
        assert nb == 0, (
            f"{nb} entrées cache pour driving/transit — ces modes ne devraient pas être calculés"
        )


# ══════════════════════════════════════════════════════════════════
# Tests Delta Lake Silver
# ══════════════════════════════════════════════════════════════════

class TestSilverDelta:

    def test_silver_dossier_existe(self):
        """Le dossier Delta Lake Silver doit exister."""
        from config import BRONZE_DIR
        silver_path = BRONZE_DIR.parent / "silver" / "employees"
        if not silver_path.exists():
            pytest.skip("Silver pas encore écrit — lancez run_round2.py")
        assert (silver_path / "_delta_log").exists(), "_delta_log absent"

    def test_silver_readable_duckdb(self):
        """DuckDB doit pouvoir lire le Silver."""
        from config import BRONZE_DIR
        import duckdb
        silver_path = str(BRONZE_DIR.parent / "silver" / "employees")
        if not Path(silver_path).exists():
            pytest.skip("Silver pas encore écrit")
        conn = duckdb.connect()
        conn.execute("INSTALL delta; LOAD delta;")
        df = conn.execute(
            f"SELECT COUNT(*) AS n FROM delta_scan('{silver_path}')"
        ).fetchdf()
        conn.close()
        assert df["n"].iloc[0] > 0

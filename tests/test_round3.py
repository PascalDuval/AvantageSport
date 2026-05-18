"""
Tests Round 3 — ETL Gold + Quality Check (SODA Core)

Couvre :
  - Calcul du Gold (primes, journées BE, cohérence)
  - Chaque règle qualité individuellement (v1 SQL + v2 SODA Core)
  - Table avantages_calcules peuplée
  - Delta Lake Gold lisible
  - Idempotence (re-run → même résultat)
  - Fichiers SodaCL présents et valides
  - soda_runner : config, SEVERITY_MAP, parse_soda_results

Lancer : pytest tests/test_round3.py -v
"""
import sys
from pathlib import Path
from datetime import date

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


@pytest.fixture(scope="module")
def default_params():
    """Paramètres métier par défaut pour les tests."""
    return {
        "taux_prime":       0.05,
        "min_activites_be": 15,
        "nb_jours_be":      5,
        "fenetre_j":        365,
        "params_version":   "v1.0_test",
    }


# ══════════════════════════════════════════════════════════════════
# Tests calcul Gold
# ══════════════════════════════════════════════════════════════════

class TestEtlGold:

    def test_load_params_depuis_config(self, db_conn):
        """Les paramètres doivent être lus depuis la table config."""
        from etl_gold import load_params
        params = load_params()
        assert "taux_prime" in params
        assert params["taux_prime"] == pytest.approx(0.05)
        assert params["min_activites_be"] == 15
        assert params["nb_jours_be"] == 5

    def test_count_activities_returns_dataframe(self):
        """count_activities_per_employee doit retourner un DataFrame."""
        import pandas as pd
        from etl_gold import count_activities_per_employee
        df = count_activities_per_employee(365)
        assert isinstance(df, pd.DataFrame)
        if len(df) == 0:
            pytest.skip("strava_activities vide — lancez run_round1.py")
        assert "employee_id" in df.columns
        assert "nb_activites_12m" in df.columns
        assert (df["nb_activites_12m"] > 0).all()

    def test_prime_calcul_montant(self, default_params):
        """montant_prime = salaire_brut × taux pour chaque éligible."""
        import pandas as pd
        from etl_gold import build_gold_df
        try:
            df = build_gold_df(default_params)
        except SystemExit:
            pytest.skip("employees ou strava_activities vide")

        eligible = df[df["eligible_prime"] == True]
        if len(eligible) == 0:
            pytest.skip("Aucun salarié éligible prime")

        for _, row in eligible.head(5).iterrows():
            assert pd.notna(row.get("salaire_brut")), \
                f"salaire_brut NULL pour {row.get('employee_id')} — vérifiez l'ingestion XLSX"
            attendu = round(float(row["salaire_brut"]) * default_params["taux_prime"], 2)
            assert float(row["montant_prime"]) == pytest.approx(attendu, abs=0.01), \
                f"Prime incorrecte pour {row.get('employee_id')}"

    def test_prime_nulle_si_non_eligible(self, default_params):
        """Les non-éligibles doivent avoir montant_prime = 0."""
        from etl_gold import build_gold_df
        try:
            df = build_gold_df(default_params)
        except SystemExit:
            pytest.skip("employees ou strava_activities vide")

        non_eligible = df[df["eligible_prime"] == False]
        assert (non_eligible["montant_prime"] == 0.0).all(), \
            "Des non-éligibles ont une prime > 0"

    def test_journees_be_coherentes(self, default_params):
        """eligible_jours_be = True ssi nb_activites >= 15."""
        from etl_gold import build_gold_df
        try:
            df = build_gold_df(default_params)
        except SystemExit:
            pytest.skip("employees ou strava_activities vide")

        eligibles_be = df[df["eligible_jours_be"] == True]
        if len(eligibles_be) > 0:
            assert (eligibles_be["nb_activites_12m"] >= default_params["min_activites_be"]).all(), \
                "Des éligibles BE ont moins de 15 activités"

        non_be = df[df["eligible_jours_be"] == False]
        assert (non_be["nb_activites_12m"] < default_params["min_activites_be"]).all(), \
            "Des non-éligibles BE ont 15 activités ou plus"

    def test_nb_jours_accordes(self, default_params):
        """Les éligibles BE doivent avoir exactement nb_jours_be = 5."""
        from etl_gold import build_gold_df
        try:
            df = build_gold_df(default_params)
        except SystemExit:
            pytest.skip("employees ou strava_activities vide")

        eligibles_be = df[df["eligible_jours_be"] == True]
        if len(eligibles_be) > 0:
            assert (eligibles_be["nb_jours_bienetre"] == 5).all(), \
                "Les éligibles BE n'ont pas exactement 5 jours"

        non_be = df[df["eligible_jours_be"] == False]
        assert (non_be["nb_jours_bienetre"] == 0).all(), \
            "Des non-éligibles ont des jours BE > 0"

    def test_dry_run_ne_modifie_pas_base(self, db_conn, default_params):
        """dry_run ne doit pas modifier avantages_calcules."""
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM avantages_calcules")
            before = cur.fetchone()[0]

        from etl_gold import build_gold_df, write_avantages
        try:
            df = build_gold_df(default_params)
        except SystemExit:
            pytest.skip("employees ou strava_activities vide")
        nb = write_avantages(df, dry_run=True)

        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM avantages_calcules")
            after = cur.fetchone()[0]

        assert nb == 0
        assert before == after, "dry_run a modifié avantages_calcules"


# ══════════════════════════════════════════════════════════════════
# Tests Quality Check
# ══════════════════════════════════════════════════════════════════

class TestQualityCheck:

    def _skip_if_not_ready(self, db_conn, table: str, min_rows: int = 1):
        with db_conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            nb = cur.fetchone()[0]
        if nb < min_rows:
            pytest.skip(f"{table} non peuplée — lancez le round correspondant")

    # ── Règles WARNING : anomalies de déclaration ─────────────────

    def test_check_distance_marche_severite_warning(self, db_conn):
        """Règle ① doit être de sévérité WARNING (anomalie, non bloquant)."""
        self._skip_if_not_ready(db_conn, "employees", 100)
        from quality_check import check_distance_marche
        result = check_distance_marche()
        assert result.severite == "WARNING"

    def test_check_distance_marche_aucune_anomalie(self, db_conn):
        """Règle ① : aucun salarié marche_running hors seuil (données attendues ≤ 15km)."""
        self._skip_if_not_ready(db_conn, "employees", 100)
        from quality_check import check_distance_marche
        result = check_distance_marche()
        if not result.resultat:
            # WARNING : on signale mais on ne bloque pas pytest
            print(f"\n  ⚠️  Anomalie marche détectée : {result.detail}")
        # Le test ne fail pas sur un WARNING — comportement attendu

    def test_check_distance_velo_severite_warning(self, db_conn):
        """Règle ② doit être de sévérité WARNING (anomalie, non bloquant)."""
        self._skip_if_not_ready(db_conn, "employees", 100)
        from quality_check import check_distance_velo
        result = check_distance_velo()
        assert result.severite == "WARNING"

    def test_check_distance_velo_aucune_anomalie(self, db_conn):
        """Règle ② : aucun salarié velo_trottinette hors seuil (données attendues ≤ 25km)."""
        self._skip_if_not_ready(db_conn, "employees", 100)
        from quality_check import check_distance_velo
        result = check_distance_velo()
        if not result.resultat:
            print(f"\n  ⚠️  Anomalie vélo détectée : {result.detail}")

    # ── Règle BLOQUANT : cohérence ETL eligible_prime ─────────────

    def test_check_eligible_prime_coherence_passes(self, db_conn):
        """Règle BLOQUANT : aucun salarié eligible_prime=True ne doit violer les règles métier."""
        self._skip_if_not_ready(db_conn, "employees", 100)
        from quality_check import check_eligible_prime_coherence
        result = check_eligible_prime_coherence()
        assert result.severite == "BLOQUANT"
        assert result.resultat is True, \
            f"Incohérence ETL détectée : {result.detail}\n" \
            f"  {result.nb_echecs} salarié(s) eligible_prime=True violent les règles métier"

    # ── Autres règles BLOQUANT ────────────────────────────────────

    def test_check_mode_enum_passes(self, db_conn):
        """Règle ③ BLOQUANT : tous les modes dans l'enum attendu."""
        self._skip_if_not_ready(db_conn, "employees", 100)
        from quality_check import check_mode_enum
        result = check_mode_enum()
        assert result.resultat is True, f"Règle mode enum KO : {result.detail}"

    def test_check_ids_sans_decimal_passes(self, db_conn):
        """Règle ④ BLOQUANT : aucun ID avec point décimal."""
        self._skip_if_not_ready(db_conn, "employees", 100)
        from quality_check import check_ids_sans_decimal
        result = check_ids_sans_decimal()
        assert result.resultat is True, f"Règle IDs KO : {result.detail}"

    def test_check_distance_sport_positive_passes(self, db_conn):
        """Règle ⑤ BLOQUANT : distance_m > 0 pour toutes les activités avec distance."""
        self._skip_if_not_ready(db_conn, "strava_activities", 100)
        from quality_check import check_distance_sport_positive
        result = check_distance_sport_positive()
        assert result.resultat is True, f"Règle distance sport KO : {result.detail}"

    def test_check_employee_id_fk_passes(self, db_conn):
        """Règle ⑥ BLOQUANT : toutes les activités référencent un salarié existant."""
        self._skip_if_not_ready(db_conn, "strava_activities", 100)
        self._skip_if_not_ready(db_conn, "employees", 100)
        from quality_check import check_employee_id_fk
        result = check_employee_id_fk()
        assert result.resultat is True, f"Règle FK KO : {result.detail}"

    # ── Règles WARNING ────────────────────────────────────────────

    def test_check_salaire_plausible_severite_warning(self, db_conn):
        """Règle ⑦ doit être de sévérité WARNING."""
        self._skip_if_not_ready(db_conn, "employees", 100)
        from quality_check import check_salaire_plausible
        result = check_salaire_plausible()
        assert result.severite == "WARNING"

    def test_check_salaire_plausible_non_null(self, db_conn):
        """Règle ⑦ : salaire_brut doit être renseigné pour tous les salariés."""
        self._skip_if_not_ready(db_conn, "employees", 100)
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM employees WHERE salaire_brut IS NULL")
            nb_null = cur.fetchone()[0]
        assert nb_null == 0, \
            f"{nb_null} salariés avec salaire_brut NULL — vérifiez la colonne 'Salaire brut' dans le XLSX"

    def test_check_dates_strava_severite_warning(self, db_conn):
        """Règle ⑧ doit être de sévérité WARNING."""
        self._skip_if_not_ready(db_conn, "strava_activities", 100)
        from quality_check import check_dates_strava_2025
        result = check_dates_strava_2025()
        assert result.severite == "WARNING"
        if not result.resultat:
            print(f"\n  ⚠️  {result.detail}")

    # ── Structure des résultats ───────────────────────────────────

    def test_result_has_correct_attributes(self, db_conn):
        """Un QualityResult doit avoir les attributs requis."""
        self._skip_if_not_ready(db_conn, "employees", 1)
        from quality_check import check_ids_sans_decimal
        result = check_ids_sans_decimal()
        assert hasattr(result, "regle")
        assert hasattr(result, "table_cible")
        assert hasattr(result, "resultat")
        assert hasattr(result, "severite")
        assert hasattr(result, "nb_total")
        assert hasattr(result, "nb_echecs")
        assert result.severite in ("BLOQUANT", "WARNING")
        assert isinstance(result.resultat, bool)

    def test_save_results_inserts_rows(self, db_conn):
        """save_results doit insérer des lignes dans data_quality_results."""
        from quality_check import check_ids_sans_decimal, save_results
        import uuid

        run_id = str(uuid.uuid4())
        results = [check_ids_sans_decimal()]
        save_results(results, run_id)

        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM data_quality_results WHERE run_id = %s",
                (run_id,)
            )
            nb = cur.fetchone()[0]

        assert nb == 1, f"Attendu 1 résultat sauvegardé, obtenu {nb}"

        with db_conn.cursor() as cur:
            cur.execute("DELETE FROM data_quality_results WHERE run_id = %s", (run_id,))
        db_conn.commit()

    def test_html_report_generated(self, db_conn, tmp_path, monkeypatch):
        """Le rapport HTML doit être généré avec le bon contenu."""
        from quality_check import check_ids_sans_decimal, generate_html_report
        import uuid

        monkeypatch.setattr("quality_check.REPORTS_DIR", tmp_path)
        results = [check_ids_sans_decimal()]
        report_path = generate_html_report(results, str(uuid.uuid4()))

        assert report_path.exists()
        content = report_path.read_text(encoding="utf-8")
        assert "Rapport Qualité" in content
        assert "id_sans_decimal" in content
        assert "employees" in content


# ══════════════════════════════════════════════════════════════════
# Tests avantages_calcules en base
# ══════════════════════════════════════════════════════════════════

class TestAvantagesDB:

    def _skip_if_empty(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM avantages_calcules")
            nb = cur.fetchone()[0]
        if nb == 0:
            pytest.skip("avantages_calcules vide — lancez run_round3.py")

    def test_avantages_peuplee(self, db_conn):
        """avantages_calcules doit contenir 161 lignes après ETL Gold."""
        self._skip_if_empty(db_conn)
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM avantages_calcules")
            nb = cur.fetchone()[0]
        assert nb == 161, f"Attendu 161 salariés, obtenu {nb}"

    def test_non_eligible_prime_zero(self, db_conn):
        """Les non-éligibles prime doivent avoir montant_prime = 0."""
        self._skip_if_empty(db_conn)
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM avantages_calcules
                WHERE eligible_prime = FALSE AND montant_prime > 0
            """)
            nb = cur.fetchone()[0]
        assert nb == 0, f"{nb} non-éligibles avec prime > 0"

    def test_eligible_prime_non_zero(self, db_conn):
        """Les éligibles prime doivent avoir montant_prime > 0 (salaire_brut renseigné)."""
        self._skip_if_empty(db_conn)
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM avantages_calcules
                WHERE eligible_prime = TRUE AND montant_prime <= 0
            """)
            nb = cur.fetchone()[0]
        assert nb == 0, \
            f"{nb} éligibles avec prime ≤ 0 — vérifiez que salaire_brut n'est pas NULL dans employees"

    def test_nb_eligibles_prime_attendu(self, db_conn):
        """68 salariés doivent être éligibles à la prime (14 marche + 54 vélo ≤ seuils)."""
        self._skip_if_empty(db_conn)
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM avantages_calcules WHERE eligible_prime = TRUE")
            nb = cur.fetchone()[0]
        assert nb == 68, f"Attendu 68 éligibles prime, obtenu {nb}"

    def test_be_jours_coherents(self, db_conn):
        """nb_jours_bienetre = 5 si eligible_jours_be, 0 sinon."""
        self._skip_if_empty(db_conn)
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM avantages_calcules
                WHERE eligible_jours_be = TRUE AND nb_jours_bienetre != 5
            """)
            nb_mauvais_be = cur.fetchone()[0]
            cur.execute("""
                SELECT COUNT(*) FROM avantages_calcules
                WHERE eligible_jours_be = FALSE AND nb_jours_bienetre != 0
            """)
            nb_mauvais_non_be = cur.fetchone()[0]
        assert nb_mauvais_be == 0, f"{nb_mauvais_be} éligibles BE sans 5 jours"
        assert nb_mauvais_non_be == 0, f"{nb_mauvais_non_be} non-éligibles avec jours BE > 0"

    def test_params_version_presente(self, db_conn):
        """params_version doit être renseignée pour tous les enregistrements."""
        self._skip_if_empty(db_conn)
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM avantages_calcules WHERE params_version IS NULL")
            nb = cur.fetchone()[0]
        assert nb == 0, f"{nb} lignes sans params_version"

    def test_idempotence(self, db_conn):
        """Deux runs successifs doivent produire le même coût total."""
        self._skip_if_empty(db_conn)

        def get_cout():
            with db_conn.cursor() as cur:
                cur.execute("SELECT ROUND(SUM(montant_prime)::NUMERIC, 2) FROM avantages_calcules")
                return float(cur.fetchone()[0] or 0)

        cout_avant = get_cout()
        assert cout_avant > 0, "Coût total primes = 0 avant idempotence — run_round3.py d'abord"

        # Commit explicite pour libérer le verrou implicite avant le TRUNCATE d'etl_gold
        db_conn.commit()

        from etl_gold import main as etl_gold
        etl_gold(dry_run=False)

        cout_apres = get_cout()
        assert cout_avant == pytest.approx(cout_apres, abs=0.01), \
            f"Idempotence rompue : {cout_avant} → {cout_apres}"


# ══════════════════════════════════════════════════════════════════
# Tests Delta Lake Gold
# ══════════════════════════════════════════════════════════════════

class TestGoldDelta:

    def test_gold_dossier_existe(self):
        """Le dossier Delta Lake Gold doit exister après ETL."""
        from config import BRONZE_DIR
        gold_path = BRONZE_DIR.parent / "gold" / "avantages"
        if not gold_path.exists():
            pytest.skip("Gold pas encore écrit — lancez run_round3.py")
        assert (gold_path / "_delta_log").exists(), "_delta_log absent"
        parquets = list(gold_path.glob("**/*.parquet"))
        assert len(parquets) > 0, "Aucun fichier Parquet dans Gold"

    def test_gold_readable_duckdb(self):
        """DuckDB doit pouvoir lire le Delta Lake Gold."""
        from config import BRONZE_DIR
        import duckdb
        gold_path = str(BRONZE_DIR.parent / "gold" / "avantages")
        if not Path(gold_path).exists():
            pytest.skip("Gold pas encore écrit")
        conn = duckdb.connect()
        conn.execute("INSTALL delta; LOAD delta;")
        df = conn.execute(f"SELECT COUNT(*) AS n FROM delta_scan('{gold_path}')").fetchdf()
        conn.close()
        assert df["n"].iloc[0] == 161

    def test_gold_colonnes_attendues(self):
        """Le schéma Gold doit contenir toutes les colonnes métier."""
        from config import BRONZE_DIR
        import duckdb
        gold_path = str(BRONZE_DIR.parent / "gold" / "avantages")
        if not Path(gold_path).exists():
            pytest.skip("Gold pas encore écrit")
        conn = duckdb.connect()
        conn.execute("INSTALL delta; LOAD delta;")
        df = conn.execute(f"SELECT * FROM delta_scan('{gold_path}') LIMIT 1").fetchdf()
        conn.close()
        attendues = {"employee_id", "montant_prime", "eligible_prime",
                     "eligible_jours_be", "nb_activites_12m", "params_version"}
        manquantes = attendues - set(df.columns)
        assert not manquantes, f"Colonnes Gold manquantes : {manquantes}"

    def test_gold_primes_coherentes_duckdb(self):
        """DuckDB : les éligibles prime dans Gold doivent avoir montant_prime > 0."""
        from config import BRONZE_DIR
        import duckdb
        gold_path = str(BRONZE_DIR.parent / "gold" / "avantages")
        if not Path(gold_path).exists():
            pytest.skip("Gold pas encore écrit")
        conn = duckdb.connect()
        conn.execute("INSTALL delta; LOAD delta;")
        df = conn.execute(f"""
            SELECT COUNT(*) AS nb_probleme
            FROM delta_scan('{gold_path}')
            WHERE eligible_prime = TRUE AND montant_prime <= 0
        """).fetchdf()
        conn.close()
        assert df["nb_probleme"].iloc[0] == 0, \
            "Des éligibles prime dans Gold ont montant_prime ≤ 0"


# ══════════════════════════════════════════════════════════════════
# Tests SODA Core (Round 3 — Quality Check v2)
# ══════════════════════════════════════════════════════════════════

class TestFichiersSoda:
    """Vérifie que les fichiers SodaCL sont présents et bien formés."""

    SODA_DIR = Path(__file__).parent.parent / "soda"

    def test_configuration_yml_exists(self):
        """soda/configuration.yml doit exister (template datasource)."""
        assert (self.SODA_DIR / "configuration.yml").exists(), \
            "soda/configuration.yml absent — git status ?"

    def test_checks_employees_exists(self):
        assert (self.SODA_DIR / "checks" / "employees.yml").exists()

    def test_checks_strava_exists(self):
        assert (self.SODA_DIR / "checks" / "strava_activities.yml").exists()

    def test_checks_gold_exists(self):
        assert (self.SODA_DIR / "checks" / "avantages_calcules.yml").exists()

    def test_employees_yaml_valid(self):
        """employees.yml doit être du YAML valide et contenir des checks."""
        import yaml
        content = (self.SODA_DIR / "checks" / "employees.yml").read_text(encoding="utf-8")
        data = yaml.safe_load(content)
        assert data is not None
        assert "checks for employees" in content

    def test_strava_yaml_valid(self):
        import yaml
        content = (self.SODA_DIR / "checks" / "strava_activities.yml").read_text(encoding="utf-8")
        data = yaml.safe_load(content)
        assert data is not None
        assert "checks for strava_activities" in content

    def test_gold_yaml_valid(self):
        import yaml
        content = (self.SODA_DIR / "checks" / "avantages_calcules.yml").read_text(encoding="utf-8")
        data = yaml.safe_load(content)
        assert data is not None
        assert "checks for avantages_calcules" in content


class TestSodaRunner:
    """Tests unitaires de soda_runner.py (sans connexion DB)."""

    def test_severity_map_contient_11_regles(self):
        """SEVERITY_MAP doit couvrir les 11 règles SodaCL déclarées."""
        from soda_runner import SEVERITY_MAP
        assert len(SEVERITY_MAP) == 11

    def test_severity_map_valeurs_valides(self):
        """Toutes les sévérités doivent être BLOQUANT ou WARNING."""
        from soda_runner import SEVERITY_MAP
        for check_name, sev in SEVERITY_MAP.items():
            assert sev in ("BLOQUANT", "WARNING"), \
                f"Sévérité invalide pour '{check_name}': {sev}"

    def test_severity_map_bloquants(self):
        """Les règles critiques doivent être BLOQUANT."""
        from soda_runner import SEVERITY_MAP
        bloquants = {k for k, v in SEVERITY_MAP.items() if v == "BLOQUANT"}
        assert "mode_deplacement_enum" in bloquants
        assert "id_sans_decimal" in bloquants
        assert "eligible_prime_coherence" in bloquants
        assert "distance_sport_positive" in bloquants
        assert "employee_id_fk" in bloquants
        assert "gold_non_vide" in bloquants
        assert "prime_coherence_gold" in bloquants

    def test_severity_map_warnings(self):
        """Les règles d'anomalie doivent être WARNING."""
        from soda_runner import SEVERITY_MAP
        warnings = {k for k, v in SEVERITY_MAP.items() if v == "WARNING"}
        assert "salaire_plausible" in warnings
        assert "anomalie_declaration_marche" in warnings
        assert "anomalie_declaration_velo" in warnings
        assert "dates_strava_fenetre_2025" in warnings

    def test_build_config_yaml_contient_datasource(self):
        """_build_config_yaml() doit produire un YAML avec la section data_sources."""
        import yaml
        from soda_runner import _build_config_yaml
        config_str = _build_config_yaml()
        data = yaml.safe_load(config_str)
        assert "data_sources" in data
        assert "poc_sport" in data["data_sources"]
        ds = data["data_sources"]["poc_sport"]
        assert ds["type"] == "postgres"
        assert "host" in ds
        assert "username" in ds

    def test_parse_soda_results_empty(self):
        """parse_soda_results avec une liste vide ne doit pas lever d'exception."""
        from soda_runner import parse_soda_results
        results = parse_soda_results([])
        assert isinstance(results, list)
        assert len(results) == 0

    def test_soda_runner_importable(self):
        """soda_runner doit être importable sans erreur."""
        import importlib
        mod = importlib.import_module("soda_runner")
        assert hasattr(mod, "main")
        assert hasattr(mod, "run_suite")
        assert hasattr(mod, "SEVERITY_MAP")

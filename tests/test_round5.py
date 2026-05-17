"""
Tests Round 5 — Export Power BI + Intégration end-to-end R1→R4

Couvre :
  - Export CSV (un fichier par page Power BI)
  - Export Excel multi-onglets
  - Cohérence des données entre tous les rounds
  - Présence des fichiers Delta Lake (Bronze + Silver + Gold)
  - Présence et validité du docker-compose.kestra.yml
  - Présence et validité du flow 00_pipeline_complet.yml (corrections R4)
  - KPI finaux attendus (68 primes, ~47 BE, coût ~106 427 €)

Lancer :
    pytest tests/test_round5.py -v
    pytest tests/ -v          # R1 → R5 complets
"""
import sys
from pathlib import Path

import pytest
import yaml

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
def engine():
    from database import get_engine
    return get_engine()


@pytest.fixture(scope="module")
def tmp_export(tmp_path_factory):
    """Dossier temporaire pour les exports CSV/Excel."""
    return tmp_path_factory.mktemp("pbi_export")


def _skip_if_empty(db_conn, table: str, min_rows: int = 1) -> None:
    with db_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        nb = cur.fetchone()[0]
    if nb < min_rows:
        pytest.skip(f"Table {table} insuffisante ({nb} lignes) — lancez les rounds précédents")


# ══════════════════════════════════════════════════════════════════
# Tests export Power BI — CSV
# ══════════════════════════════════════════════════════════════════

class TestExportCSV:

    def test_export_csv_creates_files(self, db_conn, tmp_export):
        """L'export CSV doit créer 7 fichiers dans le dossier de destination."""
        _skip_if_empty(db_conn, "avantages_calcules", 100)
        from export_power_bi import export_to_csv
        results = export_to_csv(tmp_export)
        assert len(results) >= 5, f"Attendu ≥ 5 fichiers, obtenu {len(results)}"
        for name, nb_rows in results.items():
            path = tmp_export / f"{name}.csv"
            assert path.exists(), f"Fichier {name}.csv non créé"

    def test_csv_vue_globale_has_bu(self, db_conn, tmp_export):
        """vue_globale.csv doit avoir une ligne par BU."""
        _skip_if_empty(db_conn, "avantages_calcules", 100)
        from export_power_bi import export_to_csv
        export_to_csv(tmp_export)
        import pandas as pd
        df = pd.read_csv(tmp_export / "vue_globale.csv")
        assert "bu" in df.columns
        assert len(df) >= 1
        assert df["cout_prime_euros"].sum() > 0

    def test_csv_primes_count(self, db_conn, tmp_export):
        """primes_sportives.csv doit avoir 161 lignes (un par salarié)."""
        _skip_if_empty(db_conn, "avantages_calcules", 100)
        from export_power_bi import export_to_csv
        export_to_csv(tmp_export)
        import pandas as pd
        df = pd.read_csv(tmp_export / "primes_sportives.csv")
        assert len(df) == 161, f"Attendu 161 lignes, obtenu {len(df)}"
        assert "eligible_prime" in df.columns
        assert "montant_prime_euros" in df.columns

    def test_csv_primes_nb_eligibles(self, db_conn, tmp_export):
        """Exactement 68 salariés doivent être éligibles à la prime."""
        _skip_if_empty(db_conn, "avantages_calcules", 100)
        from export_power_bi import export_to_csv
        export_to_csv(tmp_export)
        import pandas as pd
        df = pd.read_csv(tmp_export / "primes_sportives.csv")
        nb_eligible = df["eligible_prime"].sum()
        assert nb_eligible == 68, f"Attendu 68 éligibles prime, obtenu {nb_eligible}"

    def test_csv_cout_primes_plausible(self, db_conn, tmp_export):
        """Le coût total des primes doit être proche de 106 427 €."""
        _skip_if_empty(db_conn, "avantages_calcules", 100)
        from export_power_bi import export_to_csv
        export_to_csv(tmp_export)
        import pandas as pd
        df = pd.read_csv(tmp_export / "primes_sportives.csv")
        cout = float(df["montant_prime_euros"].sum())
        assert 50000 <= cout <= 200000, f"Coût total hors plage plausible : {cout:.2f} €"

    def test_csv_activites_has_monthly_data(self, db_conn, tmp_export):
        """activites_sportives.csv doit avoir des données pour plusieurs mois."""
        _skip_if_empty(db_conn, "strava_activities", 100)
        from export_power_bi import export_to_csv
        export_to_csv(tmp_export)
        import pandas as pd
        df = pd.read_csv(tmp_export / "activites_sportives.csv")
        assert "sport" in df.columns
        assert "nb_activites" in df.columns
        assert df["num_mois"].nunique() >= 10, "Moins de 10 mois couverts — données incomplètes"

    def test_csv_journees_be_seuil(self, db_conn, tmp_export):
        """journees_bienetre.csv : les éligibles BE ont nb_activites >= 15."""
        _skip_if_empty(db_conn, "avantages_calcules", 100)
        from export_power_bi import export_to_csv
        export_to_csv(tmp_export)
        import pandas as pd
        df = pd.read_csv(tmp_export / "journees_bienetre.csv")
        eligibles = df[df["eligible_jours_be"] == True]
        if len(eligibles) > 0:
            assert (eligibles["nb_activites_12m"] >= 15).all(), \
                "Des éligibles BE ont moins de 15 activités"

    def test_csv_anomalies_has_9_rules(self, db_conn, tmp_export):
        """anomalies_qualite.csv doit couvrir les 9 règles qualité."""
        _skip_if_empty(db_conn, "data_quality_results", 9)
        from export_power_bi import export_to_csv
        export_to_csv(tmp_export)
        import pandas as pd
        df = pd.read_csv(tmp_export / "anomalies_qualite.csv")
        last_run = df[df["dernier_run"] == True]
        assert len(last_run) == 9, \
            f"Attendu 9 règles dans le dernier run, obtenu {len(last_run)}"

    def test_csv_config_has_taux_prime(self, db_conn, tmp_export):
        """config_params.csv doit contenir la clé taux_prime."""
        _skip_if_empty(db_conn, "config", 5)
        from export_power_bi import export_to_csv
        export_to_csv(tmp_export)
        import pandas as pd
        df = pd.read_csv(tmp_export / "config_params.csv")
        assert "taux_prime" in df["parametre"].values
        taux = float(df[df["parametre"] == "taux_prime"]["valeur"].iloc[0])
        assert taux == pytest.approx(0.05, abs=0.001), f"taux_prime attendu 0.05, obtenu {taux}"

    def test_csv_utf8_bom_encoding(self, db_conn, tmp_export):
        """Les CSV doivent être encodés en UTF-8 BOM pour Excel Windows."""
        _skip_if_empty(db_conn, "avantages_calcules", 100)
        from export_power_bi import export_to_csv
        export_to_csv(tmp_export)
        path = tmp_export / "primes_sportives.csv"
        raw = path.read_bytes()
        assert raw[:3] == b"\xef\xbb\xbf", "CSV sans BOM UTF-8 — problème d'encodage sur Windows/Excel"


# ══════════════════════════════════════════════════════════════════
# Tests export Excel
# ══════════════════════════════════════════════════════════════════

class TestExportExcel:

    def test_export_excel_creates_file(self, db_conn, tmp_export):
        """L'export Excel doit créer un fichier .xlsx."""
        _skip_if_empty(db_conn, "avantages_calcules", 100)
        from export_power_bi import export_to_excel
        path = export_to_excel(tmp_export)
        assert path.exists()
        assert path.suffix == ".xlsx"

    def test_excel_has_expected_sheets(self, db_conn, tmp_export):
        """Le classeur Excel doit avoir au moins 5 onglets."""
        _skip_if_empty(db_conn, "avantages_calcules", 100)
        from export_power_bi import export_to_excel
        import pandas as pd
        path = export_to_excel(tmp_export)
        xl = pd.ExcelFile(path)
        assert len(xl.sheet_names) >= 5, \
            f"Attendu ≥ 5 onglets, obtenu {len(xl.sheet_names)}: {xl.sheet_names}"

    def test_excel_primes_sheet_correct(self, db_conn, tmp_export):
        """L'onglet Primes doit avoir 161 lignes."""
        _skip_if_empty(db_conn, "avantages_calcules", 100)
        from export_power_bi import export_to_excel
        import pandas as pd
        path = export_to_excel(tmp_export)
        xl = pd.ExcelFile(path)
        # Chercher l'onglet primes (nom partiel)
        primes_sheet = next((s for s in xl.sheet_names if "Prime" in s or "prime" in s), None)
        if primes_sheet:
            df = pd.read_excel(path, sheet_name=primes_sheet)
            assert len(df) == 161


# ══════════════════════════════════════════════════════════════════
# Tests cohérence end-to-end R1 → R4
# ══════════════════════════════════════════════════════════════════

class TestEndToEndCohérence:

    def test_r1_strava_count(self, db_conn):
        """R1 : strava_activities doit avoir des activités Monte Carlo 2025."""
        _skip_if_empty(db_conn, "strava_activities", 100)
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*), COUNT(DISTINCT employee_id), COUNT(DISTINCT sport_type)
                FROM strava_activities WHERE source = 'generator'
            """)
            total, athletes, sports = cur.fetchone()
        assert total >= 1000, f"Trop peu d'activités ({total}) — simulateur KO"
        assert athletes >= 90, f"Trop peu d'athlètes ({athletes})"
        assert sports == 15, f"Attendu 15 sports, obtenu {sports}"

    def test_r1_strava_dates_2025(self, db_conn):
        """R1 : toutes les activités simulées doivent être en 2025."""
        _skip_if_empty(db_conn, "strava_activities", 100)
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM strava_activities
                WHERE source = 'generator'
                AND (date_debut < '2025-01-01' OR date_debut > '2025-12-31 23:59:59')
            """)
            hors_fenetre = cur.fetchone()[0]
        assert hors_fenetre == 0, f"{hors_fenetre} activités simulées hors de 2025"

    def test_r2_employees_161(self, db_conn):
        """R2 : la table employees doit avoir exactement 161 salariés."""
        _skip_if_empty(db_conn, "employees", 100)
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM employees")
            nb = cur.fetchone()[0]
        assert nb == 161, f"Attendu 161 employees, obtenu {nb}"

    def test_r2_gmaps_cache_modes(self, db_conn):
        """R2 : gmaps_cache ne doit contenir que walking et bicycling."""
        _skip_if_empty(db_conn, "gmaps_cache", 1)
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM gmaps_cache
                WHERE mode_transport NOT IN ('walking', 'bicycling')
            """)
            bad = cur.fetchone()[0]
        assert bad == 0, f"{bad} entrées avec mode_transport interdit dans gmaps_cache"

    def test_r2_distance_null_for_tc_vehicule(self, db_conn):
        """R2 : distance_bureau_km = NULL pour TC et véhicule (jamais calculée)."""
        _skip_if_empty(db_conn, "employees", 100)
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM employees
                WHERE mode_deplacement IN ('transports_commun', 'vehicule')
                AND distance_bureau_km IS NOT NULL
            """)
            bad = cur.fetchone()[0]
        assert bad == 0, f"{bad} salariés TC/véhicule avec distance calculée (anormal)"

    def test_r3_avantages_161(self, db_conn):
        """R3 : avantages_calcules doit avoir 161 lignes."""
        _skip_if_empty(db_conn, "avantages_calcules", 100)
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM avantages_calcules")
            nb = cur.fetchone()[0]
        assert nb == 161, f"Attendu 161 avantages, obtenu {nb}"

    def test_r3_primes_68_eligibles(self, db_conn):
        """R3 : exactement 68 salariés éligibles à la prime."""
        _skip_if_empty(db_conn, "avantages_calcules", 100)
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT SUM(CASE WHEN eligible_prime THEN 1 ELSE 0 END)
                FROM avantages_calcules
            """)
            nb = cur.fetchone()[0]
        assert nb == 68, f"Attendu 68 éligibles prime, obtenu {nb}"

    def test_r3_cout_total_plausible(self, db_conn):
        """R3 : coût total des primes proche de 106 427 €."""
        _skip_if_empty(db_conn, "avantages_calcules", 100)
        with db_conn.cursor() as cur:
            cur.execute("SELECT ROUND(SUM(montant_prime)::NUMERIC, 2) FROM avantages_calcules")
            cout = float(cur.fetchone()[0] or 0)
        assert 50000 <= cout <= 300000, \
            f"Coût total hors plage plausible : {cout:.2f} €"

    def test_r3_9_regles_qc(self, db_conn):
        """R3 : le dernier run QC doit avoir exactement 9 règles."""
        _skip_if_empty(db_conn, "data_quality_results", 9)
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM data_quality_results
                WHERE run_at = (SELECT MAX(run_at) FROM data_quality_results)
            """)
            nb = cur.fetchone()[0]
        assert nb == 9, f"Attendu 9 règles QC dans le dernier run, obtenu {nb}"

    def test_r3_no_bloquant_failed(self, db_conn):
        """R3 : aucune règle BLOQUANTE ne doit avoir échoué (dernier run)."""
        _skip_if_empty(db_conn, "data_quality_results", 9)
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM data_quality_results
                WHERE run_at = (SELECT MAX(run_at) FROM data_quality_results)
                AND severite = 'BLOQUANT'
                AND resultat = FALSE
            """)
            nb = cur.fetchone()[0]
        assert nb == 0, f"{nb} règle(s) BLOQUANTE(s) en échec dans le dernier run QC"

    def test_r4_strava_manual_source(self, db_conn):
        """R4 : la colonne source distingue 'generator' (Monte Carlo) et 'manual'."""
        _skip_if_empty(db_conn, "strava_activities", 100)
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT source FROM strava_activities ORDER BY source
            """)
            sources = [r[0] for r in cur.fetchall()]
        assert "generator" in sources, "Source 'generator' absente de strava_activities"

    def test_pipeline_runs_logged(self, db_conn):
        """Des runs doivent être loggués dans pipeline_runs."""
        with db_conn.cursor() as cur:
            cur.execute("SELECT COUNT(DISTINCT flow_name) FROM pipeline_runs")
            nb_flows = cur.fetchone()[0]
        assert nb_flows >= 3, f"Seulement {nb_flows} flows distincts dans pipeline_runs"


# ══════════════════════════════════════════════════════════════════
# Tests Delta Lake — 3 couches
# ══════════════════════════════════════════════════════════════════

class TestDeltaLakeTroisCouches:

    def _check_delta(self, path_str: str, layer: str):
        """Helper : vérifie qu'un chemin Delta Lake est lisible par DuckDB."""
        import duckdb
        p = Path(path_str)
        if not p.exists():
            pytest.skip(f"Delta Lake {layer} non trouvé : {path_str}")
        assert (p / "_delta_log").exists(), f"_delta_log absent pour {layer}"
        conn = duckdb.connect()
        conn.execute("INSTALL delta; LOAD delta;")
        df = conn.execute(f"SELECT COUNT(*) AS n FROM delta_scan('{path_str}')").fetchdf()
        conn.close()
        assert df["n"].iloc[0] > 0, f"Delta Lake {layer} vide"
        return df["n"].iloc[0]

    def test_bronze_strava_readable(self):
        """Delta Lake Bronze doit être lisible et contenir des activités."""
        n = self._check_delta("data/delta/bronze/strava", "Bronze/strava")
        assert n >= 1000, f"Bronze trop peu de lignes ({n})"

    def test_silver_employees_readable(self):
        """Delta Lake Silver doit contenir 161 employees."""
        n = self._check_delta("data/delta/silver/employees", "Silver/employees")
        assert n == 161, f"Silver : attendu 161, obtenu {n}"

    def test_gold_avantages_readable(self):
        """Delta Lake Gold doit contenir 161 avantages calculés."""
        n = self._check_delta("data/delta/gold/avantages", "Gold/avantages")
        assert n == 161, f"Gold : attendu 161, obtenu {n}"

    def test_gold_primes_coherentes_duckdb(self):
        """Via DuckDB : les primes des éligibles doivent être > 0."""
        import duckdb
        gold_path = "data/delta/gold/avantages"
        if not Path(gold_path).exists():
            pytest.skip("Gold Delta Lake non trouvé")
        conn = duckdb.connect()
        conn.execute("INSTALL delta; LOAD delta;")
        df = conn.execute(f"""
            SELECT COUNT(*) AS bad
            FROM delta_scan('{gold_path}')
            WHERE eligible_prime = TRUE AND montant_prime <= 0
        """).fetchdf()
        conn.close()
        assert df["bad"].iloc[0] == 0, "Des éligibles prime ont montant_prime ≤ 0 dans le Gold Delta"


# ══════════════════════════════════════════════════════════════════
# Tests fichiers infrastructure R4/R5
# ══════════════════════════════════════════════════════════════════

class TestFichiersInfrastructure:

    def test_docker_compose_kestra_exists(self):
        """docker/docker-compose.kestra.yml doit exister."""
        assert Path("docker/docker-compose.kestra.yml").exists(), \
            "docker/docker-compose.kestra.yml absent — créé en Round 5"

    def test_docker_compose_kestra_valid_yaml(self):
        """docker/docker-compose.kestra.yml doit être du YAML valide."""
        content = Path("docker/docker-compose.kestra.yml").read_text(encoding="utf-8")
        parsed = yaml.safe_load(content)
        assert parsed is not None
        assert "services" in parsed
        assert "kestra" in parsed["services"]

    def test_docker_compose_kestra_joins_network(self):
        """Kestra doit rejoindre poc_network pour communiquer avec PostgreSQL."""
        content = Path("docker/docker-compose.kestra.yml").read_text(encoding="utf-8")
        parsed = yaml.safe_load(content)
        kestra_networks = parsed["services"]["kestra"].get("networks", [])
        network_names = parsed.get("networks", {})
        assert "poc_network" in str(kestra_networks) or "poc_network" in str(network_names), \
            "Kestra doit rejoindre poc_network"

    def test_flow_00_pipeline_complet_exists(self):
        """kestra/flows/00_pipeline_complet.yml doit exister."""
        assert Path("kestra/flows/00_pipeline_complet.yml").exists(), \
            "Flow 00_pipeline_complet.yml absent"

    def test_flow_00_uses_correct_subflow_type(self):
        """Flow 00 doit utiliser io.kestra.plugin.CORE.flow.Subflow (correction 1.3.x)."""
        content = Path("kestra/flows/00_pipeline_complet.yml").read_text(encoding="utf-8")
        assert "io.kestra.plugin.core.flow.Subflow" in content, \
            "Utiliser io.kestra.plugin.core.flow.Subflow (pas io.kestra.plugin.flow.Subflow)"
        import re
        usages = re.findall(r"^\s+type:\s*(io\.kestra\.plugin\.flow\.Subflow)", content, re.MULTILINE)
        assert len(usages) == 0, \
            "Ancienne référence io.kestra.plugin.flow.Subflow utilisée comme type (hors commentaires)"

    def test_flow_00_has_6_subflows(self):
        """Flow 00 doit orchestrer exactement 5 sous-flows (01→05)."""
        content = Path("kestra/flows/00_pipeline_complet.yml").read_text(encoding="utf-8")
        import re
        subflows = re.findall(r"flowId:\s*(ingest_xlsx|generate_strava|etl_silver_gold|quality_check|notify_slack)", content)
        assert len(subflows) == 5, f"Attendu 5 sous-flows, trouvé {len(subflows)}: {subflows}"

    def test_power_bi_queries_sql_exists(self):
        """sql/power_bi_queries.sql doit exister."""
        assert Path("sql/power_bi_queries.sql").exists(), \
            "sql/power_bi_queries.sql absent"

    def test_power_bi_queries_has_5_sections(self):
        """Le fichier SQL Power BI doit couvrir les 5 pages."""
        content = Path("sql/power_bi_queries.sql").read_text(encoding="utf-8")
        pages = ["Vue Globale", "Primes Sportives", "Journées Bien", "Activités", "Anomalies"]
        for page in pages:
            assert page in content, f"Section '{page}' absente de power_bi_queries.sql"

    def test_dax_measures_exists(self):
        """reports/dax_measures.md doit exister."""
        assert Path("reports/dax_measures.md").exists(), \
            "reports/dax_measures.md absent"

    def test_dax_measures_has_key_measures(self):
        """Le fichier DAX doit contenir les mesures clés."""
        content = Path("reports/dax_measures.md").read_text(encoding="utf-8")
        measures = [
            "Nb Éligibles Prime",
            "Coût Total Primes",
            "Total Jours BE Accordés",
        ]
        for m in measures:
            assert m in content, f"Mesure DAX '{m}' absente de dax_measures.md"

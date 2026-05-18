"""
Script d'exécution complet — Round 3

Orchestre dans l'ordre :
  1. Vérification des pré-requis (R1 + R2 validés)
  2. Contrôles qualité (deux moteurs disponibles) :
       - SODA Core (défaut recommandé) : 11 règles SodaCL déclaratives
         → soda/checks/*.yml → data_quality_results + rapport HTML
       - Quality Check SQL maison (--no-soda) : 9 règles Python+SQL (référence)
  3. ETL Gold : calcul primes + journées BE (etl_gold.py)
  4. Vérification DuckDB Gold
  5. Rapport final

Usage:
    python scripts/run_round3.py                          # SODA + ETL Gold (défaut)
    python scripts/run_round3.py --no-soda                # Quality check SQL maison
    python scripts/run_round3.py --dry-run                # calcul sans écriture base
    python scripts/run_round3.py --skip-qc                # sauter les contrôles qualité
    python scripts/run_round3.py --fail-fast              # stoppe si QC bloquant
    python scripts/run_round3.py --params-version v2.0_taux7pct
"""
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database import ping, get_connection
from etl_gold import main as etl_gold
from quality_check import main as quality_check
try:
    from soda_runner import main as soda_check
    SODA_AVAILABLE = True
except ImportError:
    SODA_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

SEP = "=" * 65


def check_prerequisites() -> bool:
    """Vérifie que R1 et R2 sont validés avant de lancer R3."""
    print(f"\n{SEP}")
    print("  PRÉ-REQUIS — Round 3")
    print(SEP)
    ok = True

    print("  [1/4] PostgreSQL...", end=" ")
    if ping(max_retries=5, delay=2):
        print("✅")
    else:
        print("❌")
        return False

    checks = [
        ("strava_activities", "Round 1", 100),
        ("employees",         "Round 2", 100),
        ("raw_rh",            "Round 2",  10),
    ]
    i = 2
    with get_connection() as conn:
        with conn.cursor() as cur:
            for table, label, min_rows in checks:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                nb = cur.fetchone()[0]
                print(f"  [{i}/4] {table} ({label})...", end=" ")
                if nb >= min_rows:
                    print(f"✅  ({nb} lignes)")
                else:
                    msg = f"Vide ou insuffisant ({nb} lignes) — lancez run_round{'1' if 'strava' in table else '2'}.py"
                    print(f"❌  {msg}")
                    ok = False
                i += 1

    print()
    return ok


def verify_gold_duckdb() -> None:
    """Lit le Delta Lake Gold et affiche les métriques."""
    from config import BRONZE_DIR
    import duckdb

    gold_path = str(BRONZE_DIR.parent / "gold" / "avantages")
    if not Path(gold_path).exists():
        logger.warning("Delta Lake Gold non encore écrit.")
        return

    conn = duckdb.connect()
    conn.execute("INSTALL delta; LOAD delta;")

    df = conn.execute(f"""
        SELECT
            COUNT(*) AS nb_total,
            SUM(CASE WHEN eligible_prime THEN 1 ELSE 0 END) AS nb_prime,
            ROUND(SUM(montant_prime)::DECIMAL, 2) AS cout_total,
            SUM(CASE WHEN eligible_jours_be THEN 1 ELSE 0 END) AS nb_be,
            MAX(params_version) AS version
        FROM delta_scan('{gold_path}')
    """).fetchdf()
    conn.close()

    print(f"\n{'─'*55}")
    print(f"  🗃  Delta Lake Gold — vérification DuckDB")
    print(f"{'─'*55}")
    for col, val in df.iloc[0].items():
        print(f"  {col:<25} : {val}")
    print(f"{'─'*55}")


def print_final_report() -> None:
    """Rapport final Round 3."""
    print(f"\n{SEP}")
    print("  RAPPORT FINAL — ROUND 3")
    print(SEP)

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:

                cur.execute("""
                    SELECT COUNT(*), SUM(montant_prime),
                           SUM(CASE WHEN eligible_prime THEN 1 ELSE 0 END),
                           SUM(CASE WHEN eligible_jours_be THEN 1 ELSE 0 END),
                           MAX(params_version)
                    FROM avantages_calcules
                """)
                r = cur.fetchone()
                print(f"\n  💰 avantages_calcules")
                print(f"     Salariés calculés    : {r[0]}")
                print(f"     Coût total primes    : {float(r[1] or 0):,.2f} €")
                print(f"     Éligibles prime      : {r[2]}")
                print(f"     Éligibles jours BE   : {r[3]}")
                print(f"     Version paramètres   : {r[4]}")

                cur.execute("""
                    SELECT COUNT(*), SUM(CASE WHEN resultat THEN 1 ELSE 0 END)
                    FROM data_quality_results
                    WHERE run_at >= NOW() - INTERVAL '1 hour'
                """)
                qc = cur.fetchone()
                print(f"\n  ✅ data_quality_results (dernier run)")
                print(f"     Règles exécutées     : {qc[0]}")
                print(f"     Règles OK            : {qc[1]}")

                cur.execute("""
                    SELECT flow_name, statut, nb_lignes, duree_ms
                    FROM pipeline_runs ORDER BY id DESC LIMIT 5
                """)
                print(f"\n  📋 Derniers runs :")
                for row in cur.fetchall():
                    print(f"     {row[0]:<30} {row[1]:<10} {row[2]} lignes  {row[3]}ms")

    except Exception as e:
        logger.error(f"Erreur rapport : {e}")

    from config import BRONZE_DIR
    gold_dir = BRONZE_DIR.parent / "gold"
    if gold_dir.exists():
        for folder in sorted(gold_dir.iterdir()):
            if folder.is_dir():
                parquets = list(folder.glob("**/*.parquet"))
                size_mb = sum(f.stat().st_size for f in parquets) / 1024 / 1024
                print(f"\n  🗃  Delta Lake Gold : gold/{folder.name} · {len(parquets)} parquet · {size_mb:.2f} MB")

    print(f"\n{SEP}")
    print("  ✅  ROUND 3 TERMINÉ")
    print(f"     pgAdmin            : http://localhost:5050")
    print(f"     Rapport SODA HTML  : reports/soda_quality_report.html")
    print(f"     Delta Lake Gold    : data/delta/gold/avantages/")
    print(f"     → Données prêtes pour Tableau (connexion PostgreSQL live ou export Hyper)")
    print(SEP)
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Round 3 complet — Quality Check SODA + ETL Gold")
    parser.add_argument("--dry-run", action="store_true",
                        help="Calcule sans écrire en base")
    parser.add_argument("--skip-qc", action="store_true",
                        help="Sauter les contrôles qualité")
    parser.add_argument("--no-soda", action="store_true",
                        help="Utiliser quality_check.py SQL maison au lieu de SODA Core")
    parser.add_argument("--fail-fast", action="store_true",
                        help="Stoppe si un contrôle BLOQUANT échoue")
    parser.add_argument("--params-version", type=str, default=None,
                        help="Surcharge la version des paramètres")
    args = parser.parse_args()
    use_soda = not args.no_soda and SODA_AVAILABLE

    start = datetime.now()
    print(f"\n{'#'*65}")
    print(f"#  POC Avantages Sportifs — ROUND 3")
    print(f"#  Quality Check + ETL Gold")
    print(f"#  {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*65}\n")

    if not check_prerequisites():
        print("❌ Pré-requis non satisfaits.")
        sys.exit(1)

    # Étape 1 : Quality Check (SODA Core par défaut, SQL maison en fallback)
    if not args.skip_qc:
        if use_soda:
            print(f"\n{SEP}")
            print("  ÉTAPE 1/2 — Contrôles Qualité SODA Core (11 règles SodaCL)")
            print(SEP)
            pipeline_ok = soda_check(suite="all", fail_fast=args.fail_fast)
            logger.info("Rapport HTML : reports/soda_quality_report.html")
        else:
            print(f"\n{SEP}")
            engine_label = "Quality Check SQL maison (--no-soda)" if args.no_soda else "Quality Check SQL (soda-core-postgres non installé)"
            print(f"  ÉTAPE 1/2 — Contrôles Qualité — {engine_label}")
            print(SEP)
            pipeline_ok = quality_check(suite="all", fail_fast=args.fail_fast)

        if not pipeline_ok and args.fail_fast:
            print("🔴 Pipeline stoppé : règles BLOQUANTES échouées.")
            sys.exit(1)
        elif not pipeline_ok:
            print("⚠️  Des règles BLOQUANTES ont échoué — ETL Gold lancé quand même.")
    else:
        print("\n  Quality Check : SKIPPED (--skip-qc)")

    # Étape 2 : ETL Gold
    print(f"\n{SEP}")
    print("  ÉTAPE 2/2 — ETL Gold (primes + journées BE)")
    print(SEP)
    try:
        metrics = etl_gold(
            dry_run=args.dry_run,
            params_version_override=args.params_version,
        )
        print(f"\n  → Éligibles prime     : {metrics['nb_eligible_prime']}")
        print(f"  → Éligibles jours BE  : {metrics['nb_eligible_be']}")
        print(f"  → Coût total primes   : {metrics['cout_total_primes']:,.2f} €")
    except Exception as e:
        logger.error(f"Échec ETL Gold : {e}", exc_info=True)
        sys.exit(1)

    # Vérification DuckDB
    if not args.dry_run:
        verify_gold_duckdb()
        print_final_report()

    elapsed = (datetime.now() - start).total_seconds()
    print(f"  Durée totale : {elapsed:.1f}s\n")


if __name__ == "__main__":
    main()

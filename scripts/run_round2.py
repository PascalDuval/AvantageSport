"""
Script d'exécution complet — Round 2

Orchestre le pipeline de normalisation et d'enrichissement :
  1. Vérification des pré-requis
  2. Ingestion XLSX → PostgreSQL (raw_rh + raw_sport)
  3. ETL Silver (normalisation + Google Maps + eligible_prime)
  4. Écriture Delta Lake Silver
  5. Écriture Bronze RH + Sport (extension du Bronze Round 1)
  6. Rapport final

Usage:
    python scripts/run_round2.py
    python scripts/run_round2.py --dry-run
    python scripts/run_round2.py --skip-gmaps   # sans appel Google Maps
    python scripts/run_round2.py --skip-ingest  # si raw_rh déjà peuplé
"""
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database import ping, get_connection
from ingest_xlsx import main as ingest_xlsx
from etl_silver import main as etl_silver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

SEP = "=" * 65


def check_prerequisites() -> bool:
    """
    Vérifie les pré-requis spécifiques au Round 2.

    Returns:
        True si tout est OK.
    """
    print(f"\n{SEP}")
    print("  PRÉ-REQUIS — Round 2")
    print(SEP)
    ok = True

    # 1. PostgreSQL
    print("  [1/4] PostgreSQL...", end=" ")
    if ping(max_retries=5, delay=2):
        print("✅")
    else:
        print("❌")
        ok = False

    # 2. Round 1 validé (strava_activities peuplée)
    print("  [2/4] Round 1 validé (strava_activities)...", end=" ")
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM strava_activities")
                nb = cur.fetchone()[0]
        if nb > 0:
            print(f"✅  ({nb} lignes)")
        else:
            print("⚠️   Table vide — Round 1 doit être lancé d'abord")
    except Exception as e:
        print(f"❌  {e}")
        ok = False

    # 3. Fichiers XLSX
    from config import RH_FILE, SPORT_FILE
    print("  [3/4] Fichiers XLSX...", end=" ")
    if RH_FILE.exists() and SPORT_FILE.exists():
        print("✅")
    else:
        print("❌")
        if not RH_FILE.exists():
            print(f"       Manquant : {RH_FILE}")
        if not SPORT_FILE.exists():
            print(f"       Manquant : {SPORT_FILE}")
        ok = False

    # 4. Google Maps (non bloquant — mode mock disponible)
    import os
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    mock = os.getenv("GMAPS_MOCK_MODE", "true").lower() == "true"
    print("  [4/4] Google Maps API...", end=" ")
    if api_key and not mock:
        print(f"✅  (clé définie, mode RÉEL)")
    else:
        print(f"⚠️   (mode MOCK — distances simulées)")
        print(f"       Pour activer l'API réelle : GOOGLE_MAPS_API_KEY=xxx dans .env")

    print()
    return ok


def print_round2_report() -> None:
    """Rapport final Round 2."""
    print(f"\n{SEP}")
    print("  RAPPORT FINAL — ROUND 2")
    print(SEP)

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:

                # employees
                cur.execute("""
                    SELECT
                        COUNT(*) as total,
                        COUNT(sport_declare) as sportifs,
                        SUM(CASE WHEN eligible_prime THEN 1 ELSE 0 END) as eligibles_prime,
                        ROUND(AVG(distance_bureau_km)::NUMERIC, 1) as dist_moy
                    FROM employees
                """)
                r = cur.fetchone()
                print(f"\n  📊 Table employees (Silver)")
                print(f"     Total salariés        : {r[0]}")
                print(f"     Avec sport déclaré    : {r[1]}")
                print(f"     Éligibles prime 5%    : {r[2]}")
                print(f"     Distance moy bureau   : {r[3]} km")

                # raw tables
                cur.execute("SELECT COUNT(*) FROM raw_rh")
                print(f"\n  📋 Tables brutes")
                print(f"     raw_rh               : {cur.fetchone()[0]} lignes")
                cur.execute("SELECT COUNT(*) FROM raw_sport")
                print(f"     raw_sport            : {cur.fetchone()[0]} lignes")
                cur.execute("SELECT COUNT(*) FROM gmaps_cache")
                print(f"     gmaps_cache          : {cur.fetchone()[0]} entrées")

                # pipeline logs
                cur.execute("""
                    SELECT flow_name, statut, nb_lignes, duree_ms
                    FROM pipeline_runs ORDER BY id DESC LIMIT 6
                """)
                print(f"\n  📋 Derniers runs :")
                for row in cur.fetchall():
                    print(f"     {row[0]:<30} {row[1]:<10} {row[2]} lignes  {row[3]}ms")

    except Exception as e:
        logger.error(f"Erreur rapport : {e}")

    # Delta Lake Silver
    from config import BRONZE_DIR
    silver_dir = BRONZE_DIR.parent / "silver"
    print(f"\n  🗃  Delta Lake Silver ({silver_dir}) :")
    if silver_dir.exists():
        for folder in sorted(silver_dir.iterdir()):
            if folder.is_dir():
                parquets = list(folder.glob("**/*.parquet"))
                size_mb = sum(f.stat().st_size for f in parquets) / 1024 / 1024
                print(f"     silver/{folder.name:<20} {len(parquets)} parquet  {size_mb:.2f} MB")

    print(f"\n{SEP}")
    print("  ✅  ROUND 2 TERMINÉ")
    print(f"     pgAdmin    : http://localhost:5050")
    print(f"     → Table employees normalisée et enrichie")
    print(f"     → Delta Lake Silver : data/delta/silver/employees/")
    print(SEP)
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Round 2 complet")
    parser.add_argument("--dry-run", action="store_true",
                        help="Affiche sans insérer en base")
    parser.add_argument("--skip-gmaps", action="store_true",
                        help="Sauter le calcul de distances Google Maps")
    parser.add_argument("--skip-ingest", action="store_true",
                        help="Sauter l'ingestion XLSX (raw_rh déjà peuplé)")
    args = parser.parse_args()

    start = datetime.now()
    print(f"\n{'#'*65}")
    print(f"#  POC Avantages Sportifs — ROUND 2")
    print(f"#  Ingestion XLSX + ETL Silver + Google Maps")
    print(f"#  {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*65}\n")

    if not check_prerequisites():
        print("❌ Pré-requis non satisfaits.")
        sys.exit(1)

    # Étape 1 : Ingestion XLSX
    if not args.skip_ingest:
        print(f"\n{SEP}")
        print("  ÉTAPE 1/2 — Ingestion XLSX → PostgreSQL")
        print(SEP)
        try:
            results = ingest_xlsx(dry_run=args.dry_run)
            print(f"  → raw_rh    : {results['nb_rh']} lignes")
            print(f"  → raw_sport : {results['nb_sport']} lignes")
        except Exception as e:
            logger.error(f"Échec ingestion : {e}", exc_info=True)
            sys.exit(1)
    else:
        print(f"\n  Ingestion XLSX : SKIPPED (--skip-ingest)")

    # Étape 2 : ETL Silver
    print(f"\n{SEP}")
    print(f"  ÉTAPE 2/2 — ETL Silver (normalisation + Google Maps)")
    print(SEP)
    try:
        etl_silver(dry_run=args.dry_run, skip_gmaps=args.skip_gmaps)
    except Exception as e:
        logger.error(f"Échec ETL Silver : {e}", exc_info=True)
        sys.exit(1)

    # Rapport
    if not args.dry_run:
        print_round2_report()

    elapsed = (datetime.now() - start).total_seconds()
    print(f"  Durée totale : {elapsed:.1f}s\n")


if __name__ == "__main__":
    main()

"""
Script d'exécution complet — Round 1

Orchestration manuelle du Round 1 (avant Kestra) :
  1. Vérification connexion PostgreSQL
    2. Génération Monte Carlo d'un scénario annuel 2025
  3. Écriture Delta Lake Bronze
  4. Rapport final

Usage:
    python scripts/run_round1.py
    python scripts/run_round1.py --dry-run
    python scripts/run_round1.py --skip-bronze
"""
import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# Résolution des imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database import ping, get_connection
from generate_strava import main as generate_strava
from bronze_writer import main as write_bronze

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

SEPARATOR = "=" * 65


def check_prerequisites() -> bool:
    """
    Vérifie que toutes les conditions sont réunies avant de démarrer.

    Returns:
        True si tout est OK, False si une vérification échoue.
    """
    print(f"\n{SEPARATOR}")
    print("  PRÉ-REQUIS")
    print(SEPARATOR)

    ok = True

    # 1. PostgreSQL accessible
    print("  [1/3] PostgreSQL...", end=" ")
    if ping(max_retries=5, delay=2):
        print("✅")
    else:
        print("❌  Démarrez PostgreSQL : docker compose -f docker/docker-compose.postgres.yml up -d")
        ok = False

    # 2. Fichiers XLSX présents
    from config import RH_FILE, SPORT_FILE
    print("  [2/3] Fichiers XLSX...", end=" ")
    if RH_FILE.exists() and SPORT_FILE.exists():
        print("✅")
    else:
        print("❌")
        if not RH_FILE.exists():
            print(f"       Manquant : {RH_FILE}")
        if not SPORT_FILE.exists():
            print(f"       Manquant : {SPORT_FILE}")
        ok = False

    # 3. Tables PostgreSQL créées
    print("  [3/3] Tables PostgreSQL...", end=" ")
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name IN (
                        'raw_rh', 'raw_sport', 'strava_activities',
                        'employees', 'avantages_calcules', 'config',
                        'gmaps_cache', 'pipeline_runs', 'data_quality_results'
                    )
                """)
                nb = cur.fetchone()[0]
        if nb >= 9:
            print(f"✅  ({nb} tables)")
        else:
            print(f"⚠️   Seulement {nb}/9 tables trouvées")
            print("       Le schéma devrait se créer automatiquement au démarrage Docker.")
            print("       Sinon : psql -h localhost -U admin -d poc_sport -f sql/init.sql")
    except Exception as e:
        print(f"❌  {e}")
        ok = False

    print()
    return ok


def print_final_report() -> None:
    """
    Affiche le rapport final du Round 1 avec toutes les statistiques.
    """
    print(f"\n{SEPARATOR}")
    print("  RAPPORT FINAL — ROUND 1")
    print(SEPARATOR)

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:

                # strava_activities
                cur.execute("""
                    SELECT
                        COUNT(*) AS total,
                        COUNT(DISTINCT employee_id) AS athletes,
                        COUNT(DISTINCT sport_type) AS sports,
                        MIN(date_debut)::date AS debut,
                        MAX(date_debut)::date AS fin
                    FROM strava_activities
                """)
                r = cur.fetchone()
                print(f"\n  📊 strava_activities")
                print(f"     Lignes totales       : {r[0]}")
                print(f"     Athlètes distincts   : {r[1]}")
                print(f"     Sports distincts     : {r[2]}")
                print(f"     Période couverte     : {r[3]} → {r[4]}")

                cur.execute("""
                    SELECT COUNT(*)
                    FROM (
                        SELECT employee_id
                        FROM strava_activities
                        WHERE date_debut::date BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'
                        GROUP BY employee_id
                        HAVING COUNT(*) > 15
                    ) t
                """)
                nb_plus_15 = cur.fetchone()[0]
                print(f"     Employés >15 act/an  : {nb_plus_15}")

                # Scénario retenu (metadata du dernier run generate_strava)
                cur.execute("""
                    SELECT id, nb_lignes, metadata
                    FROM pipeline_runs
                    WHERE flow_name = 'generate_strava' AND statut = 'SUCCESS'
                    ORDER BY id DESC
                    LIMIT 1
                """)
                scenario_row = cur.fetchone()
                if scenario_row:
                    metadata = scenario_row[2]
                    if isinstance(metadata, str):
                        metadata = json.loads(metadata)
                    metadata = metadata or {}

                    print(f"\n  🎯 Simulation")
                    print(f"     Run pipeline id      : {scenario_row[0]}")
                    print(f"     Année simulée        : {metadata.get('simulation_year', 2025)}")
                    print(f"     Act. par mois/salarié: 0 à {metadata.get('acts_max_per_month', 5)}")
                    print(f"     Salariés simulés     : {metadata.get('declared_athletes', '?')}")
                    print(f"     Seed                 : {metadata.get('seed', '?')}")
                    print(f"     Lignes insérées      : {scenario_row[1]}")

                # Par sport
                cur.execute("""
                    SELECT sport_type, COUNT(*) AS nb
                    FROM strava_activities
                    GROUP BY sport_type ORDER BY nb DESC
                """)
                print(f"\n     Détail par sport :")
                for row in cur.fetchall():
                    print(f"     {'  '+row[0]:<25} {row[1]:>4} activités")

                # Top 15 employés
                cur.execute("""
                    SELECT employee_id, COUNT(*) AS nb_activites
                    FROM strava_activities
                    GROUP BY employee_id
                    ORDER BY nb_activites DESC, employee_id
                    LIMIT 15
                """)
                print(f"\n  🏅 Top 15 employee_id (année) :")
                for rank, row in enumerate(cur.fetchall(), start=1):
                    print(f"     {rank:>2}. {row[0]:<12} {row[1]:>4} activités")

                # Extrait réel aléatoire de 100 lignes complètes
                cur.execute("""
                    SELECT
                        id, employee_id, date_debut, date_fin, sport_type,
                        distance_m, duree_s, commentaire, source, created_at
                    FROM strava_activities
                    ORDER BY RANDOM()
                    LIMIT 100
                """)
                sample_rows = cur.fetchall()
                print(f"\n  🔎 Extrait réel aléatoire (100 lignes complètes) :")
                print("     id | employee_id | date_debut | date_fin | sport_type | distance_m | duree_s | commentaire | source | created_at")
                for row in sample_rows:
                    commentaire = row[7] if row[7] is not None else ""
                    commentaire = str(commentaire).replace("\n", " ")
                    print(
                        "     "
                        f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | "
                        f"{row[5]} | {row[6]} | {commentaire} | {row[8]} | {row[9]}"
                    )

                # Pipeline runs
                cur.execute("""
                    SELECT flow_name, statut, nb_lignes, duree_ms
                    FROM pipeline_runs ORDER BY id DESC LIMIT 5
                """)
                print(f"\n  📋 Derniers runs pipeline :")
                for row in cur.fetchall():
                    print(f"     {row[0]:<30} {row[1]:<10} {row[2]} lignes  {row[3]}ms")

    except Exception as e:
        logger.error(f"Erreur rapport : {e}")

    # Delta Lake Bronze
    from config import BRONZE_DIR
    print(f"\n  🗃  Delta Lake Bronze ({BRONZE_DIR}) :")
    if BRONZE_DIR.exists():
        for folder in sorted(BRONZE_DIR.iterdir()):
            if folder.is_dir():
                parquet_files = list(folder.glob("**/*.parquet"))
                log_files = list((folder / "_delta_log").glob("*.json")) if (folder / "_delta_log").exists() else []
                size_mb = sum(f.stat().st_size for f in parquet_files) / 1024 / 1024
                print(f"     bronze/{folder.name:<20} {len(parquet_files)} parquet  {len(log_files)} logs  {size_mb:.2f} MB")
    else:
        print("     (vide — écriture Bronze non encore effectuée)")

    print(f"\n{SEPARATOR}")
    print("  ✅  ROUND 1 TERMINÉ")
    print(f"     Connexion pgAdmin  : http://localhost:5050")
    print(f"     Serveur PostgreSQL : localhost:5432 / poc_sport")
    print(SEPARATOR)
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Round 1 complet")
    parser.add_argument("--dry-run", action="store_true",
                        help="Génère sans insérer en base")
    parser.add_argument("--skip-bronze", action="store_true",
                        help="Sauter l'écriture Delta Lake Bronze")
    parser.add_argument("--seed", type=int, default=None,
                        help="Seed aléatoire pour résultat reproductible")
    args = parser.parse_args()

    start = datetime.now()
    print(f"\n{'#'*65}")
    print(f"#  POC Avantages Sportifs — ROUND 1")
    print(f"#  Infra complète + Générateur Strava-like")
    print(f"#  {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*65}\n")

    # Pré-requis
    if not check_prerequisites():
        print("❌ Pré-requis non satisfaits. Corrigez les erreurs ci-dessus.")
        sys.exit(1)

    # Étape 1 : Génération Strava-like
    print(f"\n{SEPARATOR}")
    print(f"  ÉTAPE 1/2 — Simulation Strava-like annuelle 2025 (0-5 act/mois/salarié)")
    print(SEPARATOR)
    try:
        nb_acts = generate_strava(dry_run=args.dry_run, seed=args.seed)
        print(f"  → {nb_acts} activités {'simulées' if args.dry_run else 'insérées'}")
    except Exception as e:
        logger.error(f"Échec génération Strava : {e}", exc_info=True)
        sys.exit(1)

    # Étape 2 : Bronze
    if not args.skip_bronze and not args.dry_run:
        print(f"\n{SEPARATOR}")
        print(f"  ÉTAPE 2/2 — Écriture Delta Lake Bronze")
        print(SEPARATOR)
        try:
            write_bronze()
        except Exception as e:
            logger.error(f"Échec écriture Bronze : {e}", exc_info=True)
            print(f"  ⚠️  Bronze KO mais Round 1 partiellement validé")
    else:
        print(f"\n  Étape Bronze : {'SKIPPED (--skip-bronze)' if args.skip_bronze else 'SKIPPED (--dry-run)'}")

    # Rapport final
    if not args.dry_run:
        print_final_report()

    elapsed = (datetime.now() - start).total_seconds()
    print(f"  Durée totale : {elapsed:.1f}s\n")


if __name__ == "__main__":
    main()

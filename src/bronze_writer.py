"""
Bronze Writer — Round 1

Lit les données depuis PostgreSQL et les écrit en Delta Lake local
(format Parquet + _delta_log JSON).

La couche Bronze conserve les données BRUTES telles qu'elles sont
en base — aucune transformation. C'est la couche de sauvegarde
et de rejouabilité.

Usage:
    python src/bronze_writer.py
    python src/bronze_writer.py --table strava_activities
"""
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

sys.path.insert(0, str(Path(__file__).parent))
from config import BRONZE_DIR
from database import get_engine, log_run, ping

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/bronze_writer.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)


# ── Tables à écrire en Bronze ────────────────────────────────────
# Clé : nom table PostgreSQL  /  Valeur : sous-dossier Delta Lake
TABLES_BRONZE: dict[str, str] = {
    "strava_activities": "strava",
    # Round 2 ajoutera : "raw_rh": "rh",  "raw_sport": "sport"
}


def write_table_to_bronze(
    table_name: str,
    delta_path_name: str,
    mode: str = "overwrite"
) -> int:
    """
    Lit une table PostgreSQL et l'écrit en Delta Lake Bronze.

    Le chemin de destination est : data/delta/bronze/{delta_path_name}/

    Args:
        table_name:       nom de la table PostgreSQL source.
        delta_path_name:  sous-dossier dans data/delta/bronze/.
        mode:             'overwrite' (défaut) ou 'append'.

    Returns:
        int: nombre de lignes écrites.

    Raises:
        Exception: si la lecture PostgreSQL ou l'écriture Delta échoue.
    """
    delta_path = BRONZE_DIR / delta_path_name
    delta_path.mkdir(parents=True, exist_ok=True)

    logger.info(f"📖 Lecture de la table '{table_name}' depuis PostgreSQL...")
    engine = get_engine()
    df = pd.read_sql(f"SELECT * FROM {table_name}", engine)

    if df.empty:
        logger.warning(f"⚠️  Table '{table_name}' vide — rien à écrire.")
        return 0

    logger.info(f"   {len(df)} lignes lues · {len(df.columns)} colonnes")

    # Conversion des colonnes datetime pour Delta Lake
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
        df[col] = df[col].astype("datetime64[us]")  # Delta Lake requiert microseconds

    logger.info(f"🗃  Écriture Delta Lake : {delta_path} (mode={mode})...")
    write_deltalake(
        str(delta_path),
        df,
        mode=mode,
    )

    logger.info(f"✅ Bronze OK : {len(df)} lignes dans {delta_path}")
    return len(df)


def verify_bronze(delta_path_name: str) -> None:
    """
    Vérifie que la table Delta Lake Bronze est lisible par DuckDB.
    Affiche les statistiques du fichier créé.

    Args:
        delta_path_name: sous-dossier dans data/delta/bronze/.
    """
    delta_path = str(BRONZE_DIR / delta_path_name)

    logger.info(f"🔍 Vérification DuckDB : {delta_path}...")

    conn = duckdb.connect()
    conn.execute("INSTALL delta; LOAD delta;")

    result = conn.execute(f"""
        SELECT
            COUNT(*) AS nb_lignes,
            MIN(date_debut) AS premiere_activite,
            MAX(date_debut) AS derniere_activite,
            COUNT(DISTINCT employee_id) AS nb_athletes,
            COUNT(DISTINCT sport_type) AS nb_sports
        FROM delta_scan('{delta_path}')
    """).fetchdf()

    conn.close()

    print(f"\n{'─'*55}")
    print(f"  ✅ Delta Lake Bronze — {delta_path_name}")
    print(f"{'─'*55}")
    for col, val in result.iloc[0].items():
        print(f"  {col:<25} : {val}")
    print(f"{'─'*55}")

    # Vérifier le dossier _delta_log
    log_dir = BRONZE_DIR / delta_path_name / "_delta_log"
    if log_dir.exists():
        nb_logs = len(list(log_dir.glob("*.json")))
        print(f"  _delta_log               : {nb_logs} fichier(s) JSON")
    print()


def main(table_filter: str = None) -> None:
    """
    Écrit toutes les tables Bronze (ou une seule si table_filter est donné).

    Args:
        table_filter: nom de table spécifique, ou None pour tout écrire.
    """
    debut = datetime.now()

    if not ping():
        sys.exit(1)

    tables = {
        k: v for k, v in TABLES_BRONZE.items()
        if table_filter is None or k == table_filter
    }

    if not tables:
        logger.error(f"Table '{table_filter}' non trouvée dans TABLES_BRONZE")
        sys.exit(1)

    total = 0
    for table_name, delta_name in tables.items():
        logger.info(f"\n{'═'*55}")
        logger.info(f"  Table : {table_name}  →  bronze/{delta_name}")
        logger.info(f"{'═'*55}")

        nb = write_table_to_bronze(table_name, delta_name)
        total += nb

        if nb > 0:
            verify_bronze(delta_name)

    log_run(
        flow_name="bronze_writer",
        etape=f"write_delta_bronze ({', '.join(tables.keys())})",
        statut="SUCCESS",
        debut=debut,
        nb_lignes=total
    )

    logger.info(f"\n✅ Bronze Writer terminé — {total} lignes écrites au total")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Écriture Delta Lake Bronze")
    parser.add_argument(
        "--table", type=str, default=None,
        help="Table spécifique à écrire (ex: strava_activities). Toutes si absent."
    )
    args = parser.parse_args()
    main(table_filter=args.table)

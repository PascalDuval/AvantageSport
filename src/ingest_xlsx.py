"""
Ingestion XLSX → PostgreSQL — Round 2

Charge les fichiers XLSX sources dans les tables brutes PostgreSQL :
  - Donne_es_RH.xlsx    → table raw_rh     (161 salariés)
  - Donne_es_Sportive.xlsx → table raw_sport (999 lignes, 95 avec sport déclaré)

Principe : les données sont insérées SANS transformation — telles quelles
(IDs flottants, dates Excel, typos sports conservées).
La normalisation est du ressort de l'ETL Silver (etl_silver.py).

Usage:
    python src/ingest_xlsx.py
    python src/ingest_xlsx.py --table rh       # seulement RH
    python src/ingest_xlsx.py --table sport     # seulement Sport
    python src/ingest_xlsx.py --dry-run         # affiche sans insérer
"""
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent))
from config import RH_FILE, SPORT_FILE
from database import get_connection, log_run, ping

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/ingest_xlsx.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════
# 1. Lecture des fichiers XLSX
# ════════════════════════════════════════════════════════════════

def read_rh_xlsx() -> pd.DataFrame:
    """
    Lit le fichier RH et retourne un DataFrame brut.

    Toutes les colonnes sont lues comme des chaînes (dtype=str) pour
    préserver exactement les valeurs brutes : '59019.0', '33060.0', etc.
    La normalisation sera faite en Silver.

    Returns:
        DataFrame avec les colonnes brutes du fichier RH.

    Raises:
        FileNotFoundError: si le fichier est absent de data/raw/.
    """
    if not RH_FILE.exists():
        raise FileNotFoundError(
            f"Fichier RH introuvable : {RH_FILE}\n"
            f"👉 Copiez Donne_es_RH.xlsx dans data/raw/"
        )
    logger.info(f"📂 Lecture {RH_FILE.name}...")
    df = pd.read_excel(RH_FILE, engine="openpyxl", dtype=str)
    df.columns = df.columns.str.strip()
    logger.info(f"   {len(df)} lignes · {list(df.columns)}")
    return df


def read_sport_xlsx() -> pd.DataFrame:
    """
    Lit le fichier Sport et retourne un DataFrame brut.

    Le fichier contient 999 lignes dont ~95 avec un sport non-nul.
    Les NULL sont conservés — le filtrage se fait au niveau Silver.

    Returns:
        DataFrame avec colonnes : ID salarié, Pratique d'un sport.
    """
    if not SPORT_FILE.exists():
        raise FileNotFoundError(
            f"Fichier Sport introuvable : {SPORT_FILE}\n"
            f"👉 Copiez Donne_es_Sportive.xlsx dans data/raw/"
        )
    logger.info(f"📂 Lecture {SPORT_FILE.name}...")
    df = pd.read_excel(SPORT_FILE, engine="openpyxl", dtype=str)
    df.columns = df.columns.str.strip()
    nb_sport = df["Pratique d'un sport"].notna().sum() if "Pratique d'un sport" in df.columns else "?"
    logger.info(f"   {len(df)} lignes · {nb_sport} avec sport déclaré")
    return df


# ════════════════════════════════════════════════════════════════
# 2. Insertion dans PostgreSQL
# ════════════════════════════════════════════════════════════════

def _truncate_table(table: str) -> None:
    """
    Vide une table avant réingestion (idempotence).

    Args:
        table: nom de la table PostgreSQL à vider.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY")
    logger.info(f"🗑  Table '{table}' vidée")


def insert_raw_rh(df: pd.DataFrame, dry_run: bool = False) -> int:
    """
    Insère les données RH brutes dans la table raw_rh.

    Effectue un TRUNCATE + INSERT complet (idempotent).
    Les colonnes sont mappées depuis les noms du fichier Excel vers
    les colonnes de la table PostgreSQL.

    Args:
        df:      DataFrame brut lu depuis le XLSX.
        dry_run: si True, affiche sans insérer.

    Returns:
        int: nombre de lignes insérées (0 si dry_run).
    """
    # Mapping colonnes Excel → colonnes SQL
    col_map = {
        "ID salarié":                 "id_salarie",
        "Nom":                        "nom",
        "Prénom":                     "prenom",
        "Date de naissance":          "date_naissance",
        "BU":                         "bu",
        "Date d'embauche":            "date_embauche",
        "Salaire brut":               "salaire_brut",
        "Type de contrat":            "type_contrat",
        "Nombre de jours de CP":      "nb_jours_cp",
        "Adresse du domicile":        "adresse_domicile",
        "Moyen de déplacement":       "mode_deplacement",
    }
    # Renommer les colonnes présentes (ignore les absentes)
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    if dry_run:
        logger.info(f"🧪 DRY RUN raw_rh — {len(df)} lignes (non insérées)")
        logger.info(f"   Colonnes : {list(df.columns)}")
        logger.info(f"   Exemple  : {df.iloc[0].to_dict() if len(df) > 0 else '(vide)'}")
        return 0

    source_file = str(RH_FILE.name)
    cols = [
        "id_salarie", "nom", "prenom", "date_naissance", "bu", "date_embauche",
        "salaire_brut", "type_contrat", "nb_jours_cp", "adresse_domicile",
        "mode_deplacement", "source_file"
    ]

    rows = []
    for _, row in df.iterrows():
        rows.append((
            row.get("id_salarie"),   row.get("nom"),        row.get("prenom"),
            row.get("date_naissance"), row.get("bu"),       row.get("date_embauche"),
            row.get("salaire_brut"),  row.get("type_contrat"), row.get("nb_jours_cp"),
            row.get("adresse_domicile"), row.get("mode_deplacement"),
            source_file
        ))

    _truncate_table("raw_rh")
    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO raw_rh ({', '.join(cols)}) VALUES %s",
                rows
            )
    logger.info(f"✅ raw_rh : {len(rows)} lignes insérées")
    return len(rows)


def insert_raw_sport(df: pd.DataFrame, dry_run: bool = False) -> int:
    """
    Insère les déclarations sportives brutes dans raw_sport.

    Conserve les 999 lignes (dont les ~904 avec sport NULL).
    La typo 'Runing' est conservée telle quelle — correction en Silver.

    Args:
        df:      DataFrame brut lu depuis le XLSX.
        dry_run: si True, affiche sans insérer.

    Returns:
        int: nombre de lignes insérées.
    """
    col_map = {
        "ID salarié":         "id_salarie",
        "Pratique d'un sport": "pratique_sport",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    if dry_run:
        nb_sport = df["pratique_sport"].notna().sum() if "pratique_sport" in df.columns else "?"
        logger.info(f"🧪 DRY RUN raw_sport — {len(df)} lignes ({nb_sport} avec sport)")
        return 0

    source_file = str(SPORT_FILE.name)
    rows = [
        (row.get("id_salarie"), row.get("pratique_sport"), source_file)
        for _, row in df.iterrows()
    ]

    _truncate_table("raw_sport")
    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO raw_sport (id_salarie, pratique_sport, source_file) VALUES %s",
                rows
            )

    nb_sport = sum(1 for r in rows if r[1] is not None and str(r[1]).strip().lower() != "nan")
    logger.info(f"✅ raw_sport : {len(rows)} lignes insérées ({nb_sport} avec sport déclaré)")
    return len(rows)


# ════════════════════════════════════════════════════════════════
# 3. Statistiques post-ingestion
# ════════════════════════════════════════════════════════════════

def print_ingest_stats() -> None:
    """Affiche un résumé des données ingérées en base."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            cur.execute("SELECT COUNT(*) as n FROM raw_rh")
            nb_rh = cur.fetchone()["n"]

            cur.execute("SELECT COUNT(*) as n FROM raw_rh WHERE mode_deplacement IS NOT NULL")
            nb_modes = cur.fetchone()["n"]

            cur.execute("""
                SELECT mode_deplacement, COUNT(*) as n
                FROM raw_rh GROUP BY mode_deplacement ORDER BY n DESC
            """)
            modes = cur.fetchall()

            cur.execute("SELECT COUNT(*) as n FROM raw_sport")
            nb_sport_total = cur.fetchone()["n"]

            cur.execute("""
                SELECT COUNT(*) as n FROM raw_sport
                WHERE pratique_sport IS NOT NULL AND pratique_sport != 'nan'
            """)
            nb_sport_ok = cur.fetchone()["n"]

            cur.execute("""
                SELECT pratique_sport, COUNT(*) as n
                FROM raw_sport
                WHERE pratique_sport IS NOT NULL AND pratique_sport != 'nan'
                GROUP BY pratique_sport ORDER BY n DESC
            """)
            sports = cur.fetchall()

    print(f"\n{'═'*60}")
    print(f"  📊 INGESTION — Bilan")
    print(f"{'═'*60}")
    print(f"  raw_rh   : {nb_rh} salariés ({nb_modes} avec mode déplacement)")
    print(f"\n  Modes de déplacement :")
    for m in modes:
        print(f"    {'  '+str(m['mode_deplacement']):<40} {m['n']:>4}")
    print(f"\n  raw_sport : {nb_sport_total} lignes totales / {nb_sport_ok} avec sport déclaré")
    print(f"\n  Sports déclarés :")
    for sp in sports:
        typo = " ⚠️ typo" if sp["pratique_sport"] == "Runing" else ""
        print(f"    {'  '+str(sp['pratique_sport']):<30} {sp['n']:>4}{typo}")
    print(f"{'═'*60}\n")


# ════════════════════════════════════════════════════════════════
# 4. Point d'entrée
# ════════════════════════════════════════════════════════════════

def main(table_filter: str = None, dry_run: bool = False) -> dict:
    """
    Orchestre l'ingestion complète des deux fichiers XLSX.

    Args:
        table_filter: 'rh', 'sport', ou None pour tout insérer.
        dry_run:      si True, aucune insertion.

    Returns:
        dict avec les clés 'nb_rh' et 'nb_sport'.
    """
    debut = datetime.now()

    if not ping():
        sys.exit(1)

    results = {"nb_rh": 0, "nb_sport": 0}

    # Ingestion RH
    if table_filter in (None, "rh"):
        try:
            df_rh = read_rh_xlsx()
            results["nb_rh"] = insert_raw_rh(df_rh, dry_run=dry_run)
        except FileNotFoundError as e:
            logger.error(str(e))
            if table_filter == "rh":
                sys.exit(1)

    # Ingestion Sport
    if table_filter in (None, "sport"):
        try:
            df_sport = read_sport_xlsx()
            results["nb_sport"] = insert_raw_sport(df_sport, dry_run=dry_run)
        except FileNotFoundError as e:
            logger.error(str(e))
            if table_filter == "sport":
                sys.exit(1)

    # Stats et log
    if not dry_run:
        print_ingest_stats()
        log_run(
            flow_name="ingest_xlsx",
            etape="INSERT raw_rh + raw_sport",
            statut="SUCCESS",
            debut=debut,
            nb_lignes=results["nb_rh"] + results["nb_sport"],
            metadata=results,
        )

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestion XLSX → PostgreSQL raw tables")
    parser.add_argument("--table", choices=["rh", "sport"], default=None,
                        help="Table spécifique. Toutes si absent.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Affiche sans insérer")
    args = parser.parse_args()
    main(table_filter=args.table, dry_run=args.dry_run)

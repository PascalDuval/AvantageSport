"""
Générateur Strava-like — Round 1

Pour chaque salarié ayant déclaré un sport (95 au total),
génère aléatoirement entre 0 et 5 activités par mois sur
les 12 mois de l'année 2025.

Tous les déclarants sportifs sont simulés — pas de sous-sélection.
Le nombre d'activités par mois est tiré uniformément dans [0, 5].

Usage:
    python src/generate_strava.py
    python src/generate_strava.py --dry-run
    python src/generate_strava.py --seed 42   # résultat reproductible
"""
import argparse
from calendar import monthrange
import logging
import random
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import psycopg2.extras
import unicodedata

sys.path.insert(0, str(Path(__file__).parent))
import config as cfg
from config import SPORT_PARAMS, SPORT_COMMENTS
from database import get_connection, log_run, ping

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/generate_strava.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

SIMULATION_YEAR   = 2025
ACTS_MIN_PER_MONTH = 0
ACTS_MAX_PER_MONTH = 5
COMMENT_RATE       = 0.20   # fixe — 20 % des activités ont un commentaire

GENERIC_COMMENTS = [
    "Seance reguliere bien tenue.",
    "Bonne sortie, rythme stable.",
    "Objectif du mois en bonne voie.",
    "Seance propre et sans incident.",
]


# ════════════════════════════════════════════════════════════════
# 1. Lecture des athlètes
# ════════════════════════════════════════════════════════════════

def load_athletes() -> pd.DataFrame:
    """
    Lit les fichiers XLSX et retourne les salariés ayant déclaré un sport.

    Returns:
        DataFrame avec colonnes : id_salarie, nom, prenom, sport_type

    Raises:
        FileNotFoundError: si les fichiers XLSX sont absents de data/raw/.
    """
    rh_file    = cfg.RH_FILE
    sport_file = cfg.SPORT_FILE

    if not rh_file.exists():
        raise FileNotFoundError(
            f"Fichier RH introuvable : {rh_file}\n"
            f"Copiez Donne_es_RH.xlsx dans data/raw/"
        )
    if not sport_file.exists():
        raise FileNotFoundError(
            f"Fichier Sport introuvable : {sport_file}\n"
            f"Copiez Donne_es_Sportive.xlsx dans data/raw/"
        )

    def _norm(name: str) -> str:
        nfkd = unicodedata.normalize("NFKD", str(name))
        ascii_only = "".join(c for c in nfkd if not unicodedata.combining(c))
        return "".join(c for c in ascii_only.lower() if c.isalnum())

    def _resolve(df: pd.DataFrame, candidates: list[str], src: str) -> str:
        col_map = {_norm(c): c for c in df.columns}
        for candidate in candidates:
            if _norm(candidate) in col_map:
                return col_map[_norm(candidate)]
        raise ValueError(
            f"Colonne introuvable dans {src}. "
            f"Attendu parmi : {candidates}. Trouvé : {list(df.columns)}"
        )

    logger.info(f"Lecture {rh_file.name}...")
    df_rh = pd.read_excel(rh_file, engine="openpyxl")
    df_rh.columns = df_rh.columns.str.strip()
    id_col  = _resolve(df_rh, ["ID salarié", "id_salarie", "matricule"], rh_file.name)
    nom_col = _resolve(df_rh, ["Nom", "name", "last_name"], rh_file.name)
    pre_col = _resolve(df_rh, ["Prénom", "prenom", "first_name"], rh_file.name)
    df_rh["id_salarie"] = df_rh[id_col].apply(
        lambda x: str(int(float(x))) if pd.notna(x) else None
    )

    logger.info(f"Lecture {sport_file.name}...")
    df_sport = pd.read_excel(sport_file, engine="openpyxl")
    df_sport.columns = df_sport.columns.str.strip()
    sid_col   = _resolve(df_sport, ["ID salarié", "id_salarie", "matricule"], sport_file.name)
    stype_col = _resolve(df_sport, ["Pratique d'un sport", "sport_type", "sport"], sport_file.name)
    df_sport["id_salarie"] = df_sport[sid_col].apply(
        lambda x: str(int(float(x))) if pd.notna(x) else None
    )
    df_sport = df_sport.rename(columns={stype_col: "sport_type"})

    df_athletes = df_sport[
        df_sport["sport_type"].notna()
        & (df_sport["sport_type"].astype(str).str.strip() != "")
    ].copy()
    df_athletes = df_athletes.merge(
        df_rh[["id_salarie", nom_col, pre_col]], on="id_salarie", how="left"
    ).rename(columns={nom_col: "nom", pre_col: "prenom"})

    logger.info(f"{len(df_athletes)} salariés avec sport déclaré sur {len(df_rh)} total")
    return df_athletes[["id_salarie", "nom", "prenom", "sport_type"]]


# ════════════════════════════════════════════════════════════════
# 2. Génération des activités
# ════════════════════════════════════════════════════════════════

def generate_activity(
    employee_id: str,
    sport_type: str,
    activity_date: datetime,
    rng: random.Random | None = None,
) -> dict:
    """
    Génère une activité sportive pour un salarié à une date donnée.

    Args:
        employee_id:   identifiant du salarié.
        sport_type:    type de sport (peut contenir des typos — conservé tel quel).
        activity_date: date/heure de début.
        rng:           instance Random (None → module random global).

    Returns:
        dict avec les champs de la table strava_activities.
    """
    rng = rng or random

    params = SPORT_PARAMS.get(sport_type, {"dist": None, "dur": (3600, 5400)})

    distance_m = rng.randint(*params["dist"]) if params["dist"] is not None else None
    duree_s    = rng.randint(*params["dur"])
    date_fin   = activity_date + timedelta(seconds=duree_s)

    commentaire = None
    if rng.random() < COMMENT_RATE:
        comments    = SPORT_COMMENTS.get(sport_type) or GENERIC_COMMENTS
        commentaire = rng.choice(comments)

    return {
        "employee_id": employee_id,
        "date_debut":  activity_date,
        "date_fin":    date_fin,
        "sport_type":  sport_type,
        "distance_m":  distance_m,
        "duree_s":     duree_s,
        "commentaire": commentaire,
        "source":      "generator",
    }


def generate_activities_for_athlete(
    employee_id: str,
    sport_type: str,
    year: int = SIMULATION_YEAR,
    rng: random.Random | None = None,
) -> list[dict]:
    """
    Génère entre 0 et 5 activités par mois sur les 12 mois de l'année.

    Le nombre d'activités de chaque mois est tiré indépendamment dans [0, 5].

    Args:
        employee_id: identifiant du salarié.
        sport_type:  sport pratiqué.
        year:        année civile simulée (défaut : 2025).
        rng:         instance Random (None → module random global).

    Returns:
        Liste de dicts représentant les activités du salarié sur l'année.
    """
    rng = rng or random
    activities = []

    for month in range(1, 13):
        nb_acts  = rng.randint(ACTS_MIN_PER_MONTH, ACTS_MAX_PER_MONTH)
        last_day = monthrange(year, month)[1]

        if nb_acts == 0:
            continue

        days = sorted(rng.sample(range(1, last_day + 1), k=nb_acts))
        for day in days:
            hour   = rng.choice([6, 7, 8, 12, 17, 18, 19, 20])
            minute = rng.randint(0, 59)
            activity_date = datetime(year, month, day, hour, minute)
            activities.append(
                generate_activity(employee_id, sport_type, activity_date, rng=rng)
            )

    return activities


def generate_all_activities(
    df_athletes: pd.DataFrame,
    rng: random.Random,
) -> list[dict]:
    """
    Génère les activités pour TOUS les salariés ayant déclaré un sport.

    Args:
        df_athletes: DataFrame issu de load_athletes().
        rng:         instance Random (seed fixe pour reproductibilité).

    Returns:
        Liste complète de toutes les activités à insérer.
    """
    all_acts = []
    for _, athlete in df_athletes.iterrows():
        acts = generate_activities_for_athlete(
            athlete["id_salarie"],
            athlete["sport_type"],
            rng=rng,
        )
        all_acts.extend(acts)
    logger.info(
        f"   {len(df_athletes)} salariés × moy. {len(all_acts)/len(df_athletes):.1f} act. "
        f"= {len(all_acts)} activités totales"
    )
    return all_acts


# ════════════════════════════════════════════════════════════════
# 3. Insertion PostgreSQL
# ════════════════════════════════════════════════════════════════

def purge_existing_activities() -> None:
    """Vide strava_activities avant insertion (idempotence)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE strava_activities RESTART IDENTITY")
    logger.info("Table strava_activities purgée")


def insert_activities(activities: list[dict], dry_run: bool = False) -> int:
    """
    Insère les activités dans strava_activities.

    Args:
        activities: liste de dicts générés par generate_activity().
        dry_run:    si True, affiche sans insérer.

    Returns:
        Nombre de lignes insérées (0 si dry_run).
    """
    if not activities:
        logger.warning("Aucune activité à insérer.")
        return 0

    if dry_run:
        logger.info(f"DRY RUN — {len(activities)} activités générées (aucune insertion)")
        for act in activities[:3]:
            logger.info(f"   Exemple : {act}")
        return 0

    rows = [
        (
            a["employee_id"], a["date_debut"], a["date_fin"],
            a["sport_type"],  a["distance_m"], a["duree_s"],
            a["commentaire"], a["source"],
        )
        for a in activities
    ]

    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO strava_activities
                    (employee_id, date_debut, date_fin, sport_type,
                     distance_m, duree_s, commentaire, source)
                VALUES %s
                """,
                rows,
                template="(%s, %s, %s, %s, %s, %s, %s, %s)",
            )

    logger.info(f"{len(rows)} activités insérées dans strava_activities")
    return len(rows)


# ════════════════════════════════════════════════════════════════
# 4. Statistiques post-insertion
# ════════════════════════════════════════════════════════════════

def print_stats() -> None:
    """Affiche un résumé des activités en base après insertion."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            cur.execute("SELECT COUNT(*) AS total FROM strava_activities")
            total = cur.fetchone()["total"]

            cur.execute("""
                SELECT sport_type, COUNT(*) AS nb,
                       ROUND(AVG(duree_s)/60.0, 1) AS dur_moy_min,
                       ROUND(AVG(distance_m/1000.0)::NUMERIC, 2) AS dist_moy_km
                FROM strava_activities
                GROUP BY sport_type ORDER BY nb DESC
            """)
            rows = cur.fetchall()

            cur.execute("""
                SELECT employee_id, COUNT(*) AS nb_activites
                FROM strava_activities
                GROUP BY employee_id
                ORDER BY nb_activites DESC, employee_id
                LIMIT 15
            """)
            top_athletes = cur.fetchall()

    print(f"\n{'═'*60}")
    print(f"  RÉSUMÉ strava_activities — {total} lignes totales")
    print(f"{'═'*60}")
    print(f"  {'Sport':<22} {'Nb':>5} {'Dur. moy (min)':>15} {'Dist. moy (km)':>15}")
    print(f"  {'-'*57}")
    for r in rows:
        dist_str = f"{r['dist_moy_km']:.2f}" if r["dist_moy_km"] else "  N/A  "
        print(f"  {r['sport_type']:<22} {r['nb']:>5} {str(r['dur_moy_min']):>15} {dist_str:>15}")
    print(f"\n  Top 15 salariés les plus actifs :")
    for rank, athlete in enumerate(top_athletes, start=1):
        print(f"  {rank:>2}. {athlete['employee_id']:<12} {athlete['nb_activites']:>4} activités")
    print(f"{'═'*60}\n")


# ════════════════════════════════════════════════════════════════
# 5. Point d'entrée
# ════════════════════════════════════════════════════════════════

def main(dry_run: bool = False, seed: int | None = None) -> int:
    """
    Orchestre la génération complète :
      1. Chargement des athlètes depuis XLSX
      2. Génération 0-5 activités/mois/salarié sur 2025
      3. Insertion en base
      4. Affichage des statistiques

    Args:
        dry_run: si True, génère sans insérer.
        seed:    seed aléatoire (None → seed aléatoire non reproductible).

    Returns:
        Nombre total d'activités insérées.
    """
    debut = datetime.now()

    if not ping():
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("Générateur Strava-like — Round 1")
    logger.info("   Année simulée : %s  |  0-%s act/mois/salarié  |  dry_run=%s",
                SIMULATION_YEAR, ACTS_MAX_PER_MONTH, dry_run)
    logger.info("=" * 60)

    try:
        df_athletes = load_athletes()
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)

    if df_athletes.empty:
        logger.warning("Aucun athlète trouvé. Arrêt.")
        return 0

    seed = seed or random.randint(1, 10**9)
    rng  = random.Random(seed)
    logger.info(f"   Seed : {seed}")

    logger.info(f"Génération des activités pour {len(df_athletes)} salariés...")
    all_activities = generate_all_activities(df_athletes, rng)

    nb_avec_acts = sum(
        1 for emp in df_athletes["id_salarie"]
        if any(a["employee_id"] == emp for a in all_activities)
    )
    logger.info(f"   {nb_avec_acts}/{len(df_athletes)} salariés ont au moins 1 activité")

    if not dry_run:
        purge_existing_activities()

    nb_inseres = insert_activities(all_activities, dry_run=dry_run)

    if not dry_run and nb_inseres > 0:
        print_stats()
        log_run(
            flow_name="generate_strava",
            etape="INSERT strava_activities (0-5 act/mois/salarié)",
            statut="SUCCESS",
            debut=debut,
            nb_lignes=nb_inseres,
            metadata={
                "simulation_year":     SIMULATION_YEAR,
                "acts_min_per_month":  ACTS_MIN_PER_MONTH,
                "acts_max_per_month":  ACTS_MAX_PER_MONTH,
                "declared_athletes":   len(df_athletes),
                "seed":                seed,
            },
        )

    return nb_inseres


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Générateur d'activités Strava-like")
    parser.add_argument("--dry-run", action="store_true",
                        help="Génère sans insérer en base")
    parser.add_argument("--seed", type=int, default=None,
                        help="Seed aléatoire pour résultat reproductible")
    args = parser.parse_args()

    nb = main(dry_run=args.dry_run, seed=args.seed)
    print(f"\nTerminé — {nb} activités {'simulées' if args.dry_run else 'insérées'}")

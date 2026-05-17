"""
Configuration centralisée du POC Avantages Sportifs.
Charge les variables depuis le fichier .env à la racine du projet.
Toutes les constantes métier sont ici — modifier .env sans toucher au code.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# ── Chargement du .env ───────────────────────────────────────────
# Remonte jusqu'au dossier racine du projet (2 niveaux au-dessus de src/)
ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")


# ── Chemins ─────────────────────────────────────────────────────
DATA_DIR   = ROOT_DIR / "data"
RAW_DIR    = DATA_DIR / "raw"
DELTA_DIR  = DATA_DIR / "delta"
BRONZE_DIR = DELTA_DIR / "bronze"
LOGS_DIR   = ROOT_DIR / "logs"

# Fichiers XLSX sources
RH_FILE    = RAW_DIR / "Donne_es_RH.xlsx"
SPORT_FILE = RAW_DIR / "Donne_es_Sportive.xlsx"

# Créer les dossiers s'ils n'existent pas
for d in [RAW_DIR, BRONZE_DIR, LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# ── PostgreSQL ───────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_NAME     = os.getenv("DB_NAME", "poc_sport")
DB_USER     = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin123")

# URL complète pour SQLAlchemy / psycopg2
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# ── Paramètres métier (défauts — surchargés par la table config) ─
TAUX_PRIME        = float(os.getenv("TAUX_PRIME", "0.05"))      # 5 %
SEUIL_MARCHE_KM   = float(os.getenv("SEUIL_MARCHE_KM", "15.0"))
SEUIL_VELO_KM     = float(os.getenv("SEUIL_VELO_KM", "25.0"))
MIN_ACTIVITES_BE  = int(os.getenv("MIN_ACTIVITES_BE", "15"))
NB_JOURS_BE       = int(os.getenv("NB_JOURS_BE", "5"))
ADRESSE_BUREAU    = os.getenv("ADRESSE_BUREAU", "1362 Av. des Platanes, 34970 Lattes")


# ── Générateur Strava-like ───────────────────────────────────────
STRAVA_MONTHS_BACK  = int(os.getenv("STRAVA_MONTHS_BACK", "12"))
STRAVA_MIN_ACTS     = int(os.getenv("STRAVA_MIN_ACTS", "2"))    # min act/mois
STRAVA_MAX_ACTS     = int(os.getenv("STRAVA_MAX_ACTS", "5"))    # max act/mois
STRAVA_COMMENT_RATE = float(os.getenv("STRAVA_COMMENT_RATE", "0.20"))  # 20 % avec commentaire

# Paramètres par sport : (distance_min_m, distance_max_m, duree_min_s, duree_max_s)
# None pour distance = sport sans distance mesurée
SPORT_PARAMS: dict[str, dict] = {
    "Running":         {"dist": (3000,  18000), "dur": (1080, 5400),  "unit": "km"},
    "Runing":          {"dist": (3000,  18000), "dur": (1080, 5400),  "unit": "km"},  # typo conservée
    "Randonnée":       {"dist": (5000,  20000), "dur": (3600, 14400), "unit": "km"},
    "Natation":        {"dist": (500,   3000),  "dur": (1200, 3600),  "unit": "m"},
    "Vélo":            {"dist": (10000, 50000), "dur": (2400, 10800), "unit": "km"},
    "Tennis":          {"dist": None,           "dur": (3600, 7200),  "unit": None},
    "Badminton":       {"dist": None,           "dur": (2700, 5400),  "unit": None},
    "Tennis de table": {"dist": None,           "dur": (1800, 3600),  "unit": None},
    "Football":        {"dist": None,           "dur": (3600, 5400),  "unit": None},
    "Rugby":           {"dist": None,           "dur": (3600, 5400),  "unit": None},
    "Basketball":      {"dist": None,           "dur": (3600, 5400),  "unit": None},
    "Judo":            {"dist": None,           "dur": (3600, 5400),  "unit": None},
    "Boxe":            {"dist": None,           "dur": (2700, 4500),  "unit": None},
    "Escalade":        {"dist": None,           "dur": (5400, 10800), "unit": None},
    "Triathlon":       {"dist": (10000, 40000), "dur": (3600, 10800), "unit": "km"},
    "Voile":           {"dist": None,           "dur": (7200, 18000), "unit": None},
    "Équitation":      {"dist": None,           "dur": (3600, 7200),  "unit": None},
}

# Commentaires réalistes par sport
SPORT_COMMENTS: dict[str, list[str]] = {
    "Running":    ["Belle sortie matinale !", "Sortie récupération", "Objectif semi-marathon en vue 🏃", "Pluie mais motivation au top"],
    "Runing":     ["Belle sortie matinale !", "Sortie récupération", "On continue !"],
    "Randonnée":  ["Vue magnifique depuis le sommet 🏔", "Chemin de St Guilhem le désert, je vous le conseille", "Nouvelle boucle découverte", "Les pieds dans l'eau 💧"],
    "Natation":   ["Séance de crawl", "Travail des virages", "Récupération active"],
    "Vélo":       ["Sortie Ventoux !", "Gravel dans les bois", "Le vent était de face... 😅"],
    "Tennis":     ["Match nul, belle partie", "Nouveau revers en cours d'acquisition", "Victoire 6-3 6-2 🎾"],
    "Football":   ["Match amical", "Entraînement du mardi", "Nouveau but ! ⚽"],
    "Escalade":   ["Nouvelle voie 6b ouverte", "Bloc sur plastron", "Progrès sur les surplombs 🧗"],
    "Natation":   ["Séance de crawl", "1500m non-stop !"],
}

# ── Round 2 — Google Maps ─────────────────────────────────────
import os as _os
GOOGLE_MAPS_API_KEY = _os.getenv("GOOGLE_MAPS_API_KEY")
GMAPS_MOCK_MODE     = _os.getenv("GMAPS_MOCK_MODE", "true").lower() == "true"
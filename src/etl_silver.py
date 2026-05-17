"""
ETL Silver — Round 2

Transforme les données brutes (raw_rh + raw_sport) en données
normalisées et enrichies dans la table employees (couche Silver).

Pipeline Silver :
  raw_rh + raw_sport
      ↓  [normalisation IDs, dates, modes, sports]
      ↓  [jointure RH ↔ Sport]
      ↓  [géocodage Google Maps API (ou mock)]
      ↓  [calcul eligible_prime]
      ↓  [upsert table employees]
      ↓  [écriture Delta Lake Silver]

Règles métier appliquées :
  - eligible_prime = True si mode IN (marche_running, velo_trottinette)
                         ET distance ≤ seuil (config : seuil_marche_km / seuil_velo_km)
  - Normalisation sport : 'Runing' → 'Running', strip whitespace
  - Normalisation mode : 4 valeurs → enum cible

Usage:
    python src/etl_silver.py
    python src/etl_silver.py --dry-run
    python src/etl_silver.py --skip-gmaps   # pas d'appel distance (test)
"""
import argparse
import logging
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Optional
import re

import pandas as pd
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent))
from config import BRONZE_DIR, SEUIL_MARCHE_KM, SEUIL_VELO_KM
from database import get_connection, get_config, log_run, ping, get_engine
from gmaps_client import get_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/etl_silver.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════
# 1. Fonctions de normalisation
# ════════════════════════════════════════════════════════════════

# Mapping complet des valeurs brutes du fichier → enum normalisé
MODE_DEPLACEMENT_MAP = {
    "marche/running":                "marche_running",
    "vélo/trottinette/autres":       "velo_trottinette",
    "transports en commun":          "transports_commun",
    "véhicule thermique/électrique": "vehicule",
    # Variantes possibles
    "velo/trottinette/autres":       "velo_trottinette",
    "vehicule thermique/electrique": "vehicule",
    "marche/runing":                 "marche_running",
}

# Corrections orthographiques des sports
SPORT_CORRECTIONS = {
    "runing": "Running",
    "running": "Running",
    "randonnée": "Randonnée",
    "natation": "Natation",
    "tennis": "Tennis",
    "football": "Football",
    "badminton": "Badminton",
    "basketball": "Basketball",
    "rugby": "Rugby",
    "judo": "Judo",
    "boxe": "Boxe",
    "escalade": "Escalade",
    "triathlon": "Triathlon",
    "voile": "Voile",
    "équitation": "Équitation",
    "tennis de table": "Tennis de table",
    "vélo": "Vélo",
}


def normalize_id(raw_id: Optional[str]) -> Optional[str]:
    """
    Normalise un ID salarié : '59019.0' → '59019'.

    Gère les formats : float (59019.0), entier (59019), string ('59019').

    Args:
        raw_id: identifiant brut depuis le fichier XLSX.

    Returns:
        str: identifiant normalisé, ou None si vide/invalide.
    """
    if raw_id is None or str(raw_id).strip() in ("", "nan", "None"):
        return None
    try:
        return str(int(float(str(raw_id).strip())))
    except (ValueError, TypeError):
        return str(raw_id).strip()


def normalize_date(raw_date: Optional[str]) -> Optional[date]:
    """
    Normalise une date depuis les différents formats du fichier XLSX.

    Formats gérés :
    - Serial Excel  : '33060.0' (nombre de jours depuis 1899-12-30)
    - DateTime str  : '1990-07-06 00:00:00'
    - Date str      : '1990-07-06' ou '06/07/1990'

    Args:
        raw_date: date brute depuis le fichier.

    Returns:
        date: objet date Python, ou None si non parsable.
    """
    if raw_date is None or str(raw_date).strip() in ("", "nan", "None", "NaT"):
        return None

    raw = str(raw_date).strip()

    # Cas 1 : serial Excel (ex: '33060.0')
    try:
        serial = float(raw)
        if 1000 < serial < 100000:  # plage réaliste pour une date RH
            from datetime import timedelta
            base = datetime(1899, 12, 30)
            return (base + timedelta(days=serial)).date()
    except ValueError:
        pass

    # Cas 2 : format datetime string
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue

    logger.warning(f"Date non parsable : '{raw}'")
    return None


def normalize_salaire(raw: Optional[str]) -> Optional[float]:
    """
    Normalise un salaire : '30940.0' → 30940.00

    Args:
        raw: valeur brute du salaire (peut être flottant en string).

    Returns:
        float: salaire normalisé, ou None si non numérique.
    """
    if raw is None or str(raw).strip() in ("", "nan", "None"):
        return None
    try:
        return round(float(str(raw).strip()), 2)
    except ValueError:
        return None


def normalize_mode_deplacement(raw: Optional[str]) -> Optional[str]:
    """
    Normalise le mode de déplacement vers les 4 valeurs de l'enum.

    Enum cible :
        - marche_running
        - velo_trottinette
        - transports_commun
        - vehicule

    Args:
        raw: valeur brute du fichier (ex: 'Vélo/Trottinette/Autres').

    Returns:
        str: valeur normalisée, ou None si non reconnue.
    """
    if raw is None or str(raw).strip() in ("", "nan", "None"):
        return None
    normalized = MODE_DEPLACEMENT_MAP.get(str(raw).strip().lower())
    if normalized is None:
        logger.warning(f"Mode déplacement non reconnu : '{raw}'")
    return normalized


def normalize_sport(raw: Optional[str]) -> Optional[str]:
    """
    Normalise le sport déclaré : corrections orthographiques + capitalisation.

    Correction principale : 'Runing' → 'Running' (typo dans les données source).

    Args:
        raw: sport brut depuis le fichier XLSX.

    Returns:
        str: sport normalisé, ou None si absent.
    """
    if raw is None or str(raw).strip() in ("", "nan", "None"):
        return None
    raw_clean = str(raw).strip()
    normalized = SPORT_CORRECTIONS.get(raw_clean.lower())
    if normalized:
        if raw_clean.lower() == "runing":
            logger.debug(f"Typo corrigée : '{raw_clean}' → '{normalized}'")
        return normalized
    # Fallback : capitaliser la première lettre
    return raw_clean.capitalize()


def compute_eligible_prime(
    mode: Optional[str],
    distance_km: Optional[float],
    seuil_marche: float,
    seuil_velo: float,
) -> bool:
    """
    Calcule l'éligibilité à la prime sportive 5%.

    Règle :
        eligible = mode IN ('marche_running', 'velo_trottinette')
               AND distance ≤ seuil_correspondant

    Args:
        mode:          mode de déplacement normalisé.
        distance_km:   distance domicile ↔ bureau en km.
        seuil_marche:  seuil maximum pour marche/running (config: seuil_marche_km).
        seuil_velo:    seuil maximum pour vélo/trottinette (config: seuil_velo_km).

    Returns:
        bool: True si éligible à la prime.
    """
    if mode is None or distance_km is None:
        return False
    if mode == "marche_running":
        return distance_km <= seuil_marche
    if mode == "velo_trottinette":
        return distance_km <= seuil_velo
    return False


# ════════════════════════════════════════════════════════════════
# 2. Construction du DataFrame Silver
# ════════════════════════════════════════════════════════════════

def build_silver_df(skip_gmaps: bool = False) -> pd.DataFrame:
    """
    Construit le DataFrame Silver complet depuis les tables raw.

    Étapes :
    1. Lecture raw_rh + raw_sport
    2. Normalisation de toutes les colonnes
    3. Jointure RH ↔ Sport (LEFT JOIN sur id_salarie)
    4. Calcul des distances via Google Maps (ou mock)
    5. Calcul eligible_prime

    Args:
        skip_gmaps: si True, distance_km = None (test ou debug).

    Returns:
        DataFrame Silver avec toutes les colonnes de la table employees.
    """
    engine = get_engine()

    # ── Lecture brute ────────────────────────────────────────────
    logger.info("📖 Lecture raw_rh...")
    df_rh = pd.read_sql("SELECT * FROM raw_rh", engine)
    logger.info(f"   {len(df_rh)} lignes RH brutes")

    logger.info("📖 Lecture raw_sport...")
    df_sport = pd.read_sql("""
        SELECT DISTINCT ON (id_salarie) id_salarie, pratique_sport
        FROM raw_sport
        WHERE pratique_sport IS NOT NULL AND pratique_sport != 'nan'
        ORDER BY id_salarie, id DESC
    """, engine)
    logger.info(f"   {len(df_sport)} salariés avec sport déclaré")

    # ── Normalisation RH ─────────────────────────────────────────
    logger.info("🔧 Normalisation des données RH...")

    df_rh["id"] = df_rh["id_salarie"].apply(normalize_id)
    df_rh["date_naissance_norm"] = df_rh["date_naissance"].apply(normalize_date)
    df_rh["date_embauche_norm"] = df_rh["date_embauche"].apply(normalize_date)
    df_rh["salaire_brut_norm"] = df_rh["salaire_brut"].apply(normalize_salaire)
    df_rh["mode_norm"] = df_rh["mode_deplacement"].apply(normalize_mode_deplacement)
    df_rh["nb_cp_norm"] = pd.to_numeric(
        df_rh["nb_jours_cp"].apply(lambda x: str(x).split(".")[0] if pd.notna(x) else None),
        errors="coerce"
    ).astype("Int64")

    # Log des modes de déplacement
    modes_count = df_rh["mode_norm"].value_counts(dropna=False).to_dict()
    logger.info(f"   Modes normalisés : {modes_count}")

    # ── Normalisation Sport ──────────────────────────────────────
    df_sport["id"] = df_sport["id_salarie"].apply(normalize_id)
    df_sport["sport_norm"] = df_sport["pratique_sport"].apply(normalize_sport)

    typos = df_sport[df_sport["pratique_sport"] == "Runing"]
    if len(typos) > 0:
        logger.info(f"   Typos 'Runing' corrigées : {len(typos)} cas")

    # ── Jointure ─────────────────────────────────────────────────
    logger.info("🔗 Jointure RH ↔ Sport...")
    df_silver = df_rh.merge(
        df_sport[["id", "sport_norm"]],
        on="id",
        how="left"
    )
    nb_sportifs = df_silver["sport_norm"].notna().sum()
    logger.info(f"   {len(df_silver)} employés · {nb_sportifs} avec sport")

    # ── Distances Google Maps ────────────────────────────────────
    seuil_marche = float(get_config("seuil_marche_km", SEUIL_MARCHE_KM))
    seuil_velo   = float(get_config("seuil_velo_km",   SEUIL_VELO_KM))

    if skip_gmaps:
        logger.warning("⚠️  Google Maps SKIPPED (--skip-gmaps)")
        df_silver["distance_km"] = None
        df_silver["lat"] = None
        df_silver["lng"] = None
    else:
        logger.info("🗺  Calcul distances domicile ↔ bureau...")
        client = get_client()

        # Seuls les modes éligibles ont besoin de la distance
        # transports_commun et vehicule → non éligibles, pas d'appel API
        MODES_ELIGIBLES = {"marche_running", "velo_trottinette"}
        mask_eligible = df_silver["mode_norm"].isin(MODES_ELIGIBLES)
        nb_skip = (~mask_eligible).sum()
        if nb_skip:
            logger.info(f"   {nb_skip} employés ignorés (transports_commun / vehicule) — distance inutile")

        employees_list = df_silver.loc[mask_eligible, ["id", "adresse_domicile", "mode_norm"]].to_dict("records")
        enriched = client.batch_distances(
            employees_list,
            mode_col="mode_norm",
            addr_col="adresse_domicile",
        )
        df_distances = pd.DataFrame(enriched)[["id", "distance_km", "lat", "lng"]]
        # Les non-éligibles restent avec distance_km / lat / lng à None
        df_silver["distance_km"] = None
        df_silver["lat"] = None
        df_silver["lng"] = None
        df_silver = df_silver.merge(df_distances, on="id", how="left", suffixes=("", "_new"))
        for col in ["distance_km", "lat", "lng"]:
            df_silver[col] = df_silver[f"{col}_new"].combine_first(df_silver[col])
            df_silver.drop(columns=[f"{col}_new"], inplace=True)

    # ── Éligibilité prime ────────────────────────────────────────
    df_silver["eligible_prime"] = df_silver.apply(
        lambda row: compute_eligible_prime(
            row.get("mode_norm"),
            row.get("distance_km"),
            seuil_marche,
            seuil_velo,
        ),
        axis=1,
    )

    nb_eligible = df_silver["eligible_prime"].sum()
    logger.info(f"   Éligibles prime : {nb_eligible}/{len(df_silver)}")

    return df_silver


# ════════════════════════════════════════════════════════════════
# 3. Écriture dans PostgreSQL (table employees)
# ════════════════════════════════════════════════════════════════

def write_to_employees(df: pd.DataFrame, dry_run: bool = False) -> int:
    """
    Upserte le DataFrame Silver dans la table employees.

    Utilise ON CONFLICT (id) DO UPDATE pour être idempotent.

    Args:
        df:      DataFrame Silver construit par build_silver_df().
        dry_run: si True, affiche sans insérer.

    Returns:
        int: nombre de lignes insérées/mises à jour.
    """
    if dry_run:
        logger.info(f"🧪 DRY RUN employees — {len(df)} lignes (non insérées)")
        logger.info(f"   Colonnes disponibles : {list(df.columns)}")
        return 0

    def _v(val):
        """Convertit pd.NA / np.nan en None pour psycopg2."""
        import pandas as pd
        import math
        if val is pd.NA:
            return None
        try:
            if math.isnan(val):
                return None
        except (TypeError, ValueError):
            pass
        return val

    rows = []
    for _, row in df.iterrows():
        rows.append((
            _v(row.get("id")),
            _v(row.get("nom")),
            _v(row.get("prenom")),
            _v(row.get("date_naissance_norm")),
            _v(row.get("bu")),
            _v(row.get("date_embauche_norm")),
            _v(row.get("salaire_brut_norm")),
            _v(row.get("type_contrat")),
            _v(row.get("nb_cp_norm")),
            _v(row.get("adresse_domicile")),
            _v(row.get("mode_norm")),
            _v(row.get("sport_norm")),
            _v(row.get("lat")),
            _v(row.get("lng")),
            _v(row.get("distance_km")),
            bool(row.get("eligible_prime", False)),
        ))

    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO employees
                    (id, nom, prenom, date_naissance, bu, date_embauche,
                     salaire_brut, type_contrat, nb_jours_cp, adresse_domicile,
                     mode_deplacement, sport_declare, coords_lat, coords_lng,
                     distance_bureau_km, eligible_prime)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    nom              = EXCLUDED.nom,
                    prenom           = EXCLUDED.prenom,
                    date_naissance   = EXCLUDED.date_naissance,
                    bu               = EXCLUDED.bu,
                    date_embauche    = EXCLUDED.date_embauche,
                    salaire_brut     = EXCLUDED.salaire_brut,
                    type_contrat     = EXCLUDED.type_contrat,
                    nb_jours_cp      = EXCLUDED.nb_jours_cp,
                    adresse_domicile = EXCLUDED.adresse_domicile,
                    mode_deplacement = EXCLUDED.mode_deplacement,
                    sport_declare    = EXCLUDED.sport_declare,
                    coords_lat       = EXCLUDED.coords_lat,
                    coords_lng       = EXCLUDED.coords_lng,
                    distance_bureau_km = EXCLUDED.distance_bureau_km,
                    eligible_prime   = EXCLUDED.eligible_prime,
                    updated_at       = NOW()
                """,
                rows
            )

    logger.info(f"✅ employees : {len(rows)} lignes insérées/mises à jour")
    return len(rows)


# ════════════════════════════════════════════════════════════════
# 4. Écriture Delta Lake Silver
# ════════════════════════════════════════════════════════════════

def write_silver_delta(df: pd.DataFrame) -> None:
    """
    Écrit le DataFrame Silver dans la couche Delta Lake.

    Chemin : data/delta/silver/employees/

    Args:
        df: DataFrame Silver complet.
    """
    from deltalake.writer import write_deltalake

    silver_path = BRONZE_DIR.parent / "silver" / "employees"
    silver_path.mkdir(parents=True, exist_ok=True)

    # Colonnes à conserver dans le Silver
    cols_silver = [
        "id", "nom", "prenom", "date_naissance_norm", "bu", "date_embauche_norm",
        "salaire_brut_norm", "type_contrat", "nb_cp_norm", "adresse_domicile",
        "mode_norm", "sport_norm", "lat", "lng", "distance_km", "eligible_prime"
    ]
    df_out = df[[c for c in cols_silver if c in df.columns]].copy()

    import pyarrow as pa

    # Conversion datetime → microseconds
    for col in df_out.select_dtypes(include=["datetime64[ns]"]).columns:
        df_out[col] = df_out[col].astype("datetime64[us]")

    # Colonnes date Python → string
    for col in ["date_naissance_norm", "date_embauche_norm"]:
        if col in df_out.columns:
            df_out[col] = df_out[col].apply(lambda x: str(x) if x is not None else None)

    # Pandas nullable types (Int64, Float64) → numpy float64
    for col in df_out.columns:
        if str(df_out[col].dtype) in ("Int64", "Float64"):
            df_out[col] = df_out[col].astype("float64")

    # Convertir en table Arrow puis corriger les colonnes de type Null
    # (Arrow infère Null quand toute la colonne est None — Delta Lake refuse ce type)
    tbl = pa.Table.from_pandas(df_out, preserve_index=False)
    new_cols, new_fields = [], []
    for i, field in enumerate(tbl.schema):
        col = tbl.column(i)
        if pa.types.is_null(field.type):
            col = pa.array([None] * len(col), type=pa.string())
            field = field.with_type(pa.string())
        new_cols.append(col)
        new_fields.append(field)
    tbl = pa.table(
        {f.name: c for f, c in zip(new_fields, new_cols)},
        schema=pa.schema(new_fields),
    )

    write_deltalake(str(silver_path), tbl, mode="overwrite", schema_mode="overwrite")
    logger.info(f"🗃  Delta Lake Silver écrit : {silver_path}")


# ════════════════════════════════════════════════════════════════
# 5. Rapport post-Silver
# ════════════════════════════════════════════════════════════════

def print_silver_stats() -> None:
    """Affiche un bilan Silver depuis la table employees."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            cur.execute("SELECT COUNT(*) as n FROM employees")
            nb = cur.fetchone()["n"]

            cur.execute("""
                SELECT mode_deplacement, COUNT(*) as n,
                       SUM(CASE WHEN eligible_prime THEN 1 ELSE 0 END) as eligible
                FROM employees GROUP BY mode_deplacement ORDER BY n DESC
            """)
            modes = cur.fetchall()

            cur.execute("""
                SELECT sport_declare, COUNT(*) as n
                FROM employees WHERE sport_declare IS NOT NULL
                GROUP BY sport_declare ORDER BY n DESC
            """)
            sports = cur.fetchall()

            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE eligible_prime) as nb_eligible_prime,
                    ROUND(AVG(distance_bureau_km)::NUMERIC, 1) as dist_moy,
                    ROUND(MIN(distance_bureau_km)::NUMERIC, 1) as dist_min,
                    ROUND(MAX(distance_bureau_km)::NUMERIC, 1) as dist_max
                FROM employees
                WHERE distance_bureau_km IS NOT NULL
            """)
            stats = cur.fetchone()

    print(f"\n{'═'*62}")
    print(f"  📊 SILVER — Table employees ({nb} salariés)")
    print(f"{'═'*62}")
    print(f"\n  Modes de déplacement & éligibilité prime :")
    print(f"  {'Mode':<25} {'Total':>6}  {'Éligibles prime':>16}")
    print(f"  {'-'*50}")
    for m in modes:
        mode_label = str(m["mode_deplacement"]) if m["mode_deplacement"] else "NULL"
        print(f"  {mode_label:<25} {m['n']:>6}  {m['eligible']:>16}")

    print(f"\n  Distances bureau (km) :")
    print(f"  Moy={stats['dist_moy']} · Min={stats['dist_min']} · Max={stats['dist_max']}")
    print(f"  Éligibles prime totaux : {stats['nb_eligible_prime']}")

    print(f"\n  Sports déclarés normalisés :")
    for sp in sports:
        print(f"  {'  '+str(sp['sport_declare']):<28} {sp['n']:>4}")
    print(f"{'═'*62}\n")


# ════════════════════════════════════════════════════════════════
# 6. Point d'entrée
# ════════════════════════════════════════════════════════════════

def main(dry_run: bool = False, skip_gmaps: bool = False) -> None:
    """
    Orchestre l'ETL Silver complet.

    Args:
        dry_run:    si True, construit le DataFrame mais n'écrit pas.
        skip_gmaps: si True, ne calcule pas les distances.
    """
    debut = datetime.now()

    if not ping():
        sys.exit(1)

    logger.info("=" * 62)
    logger.info("🔧 ETL Silver — Round 2")
    logger.info(f"   dry_run={dry_run} · skip_gmaps={skip_gmaps}")
    logger.info("=" * 62)

    # Vérifier que raw_rh est peuplé
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_rh")
            nb_rh = cur.fetchone()[0]
    if nb_rh == 0:
        logger.error("raw_rh est vide. Lancez d'abord : python src/ingest_xlsx.py")
        sys.exit(1)

    # Construction Silver
    df_silver = build_silver_df(skip_gmaps=skip_gmaps)

    # Écriture
    nb = write_to_employees(df_silver, dry_run=dry_run)

    if not dry_run:
        write_silver_delta(df_silver)
        print_silver_stats()
        log_run(
            flow_name="etl_silver",
            etape="build+write employees Silver",
            statut="SUCCESS",
            debut=debut,
            nb_lignes=nb,
            metadata={
                "nb_eligible_prime": int(df_silver["eligible_prime"].sum()),
                "nb_sportifs": int(df_silver["sport_norm"].notna().sum()),
                "skip_gmaps": skip_gmaps,
            }
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Silver — normalisation + enrichissement")
    parser.add_argument("--dry-run", action="store_true",
                        help="Construit le DataFrame sans écrire en base")
    parser.add_argument("--skip-gmaps", action="store_true",
                        help="Sauter le calcul de distances Google Maps")
    args = parser.parse_args()
    main(dry_run=args.dry_run, skip_gmaps=args.skip_gmaps)

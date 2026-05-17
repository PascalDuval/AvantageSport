"""
Utilitaires de connexion PostgreSQL.
Fournit :
  - get_connection()  : connexion psycopg2 brute
  - get_engine()      : moteur SQLAlchemy (pour pandas read_sql/to_sql)
  - log_run()         : enregistrer un run pipeline dans pipeline_runs
  - get_config()      : lire les paramètres métier depuis la table config
"""
import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Optional
import time

import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text

from config import DB_URL, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

logger = logging.getLogger(__name__)


# ── Connexion psycopg2 brute ─────────────────────────────────────
@contextmanager
def get_connection():
    """
    Context manager qui ouvre et ferme automatiquement une connexion psycopg2.

    Usage:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")

    Raises:
        psycopg2.OperationalError: si PostgreSQL n'est pas accessible.
    """
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        connect_timeout=10
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Moteur SQLAlchemy (pour pandas) ─────────────────────────────
_engine = None

def get_engine():
    """
    Retourne un moteur SQLAlchemy réutilisable (singleton).
    Utilisé par pandas.read_sql() et DataFrame.to_sql().

    Returns:
        sqlalchemy.Engine: moteur connecté à la base PostgreSQL.
    """
    global _engine
    if _engine is None:
        _engine = create_engine(DB_URL, echo=False)
    return _engine


# ── Ping de connexion ────────────────────────────────────────────
def ping(max_retries: int = 10, delay: int = 3) -> bool:
    """
    Vérifie que PostgreSQL est accessible. Réessaie max_retries fois.

    Args:
        max_retries: nombre de tentatives avant d'abandonner.
        delay: secondes d'attente entre chaque tentative.

    Returns:
        True si la connexion réussit, False sinon.
    """
    for attempt in range(1, max_retries + 1):
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            logger.info(f"✅ PostgreSQL accessible (tentative {attempt})")
            return True
        except psycopg2.OperationalError as e:
            logger.warning(f"⏳ PostgreSQL pas encore prêt (tentative {attempt}/{max_retries}) : {e}")
            if attempt < max_retries:
                time.sleep(delay)
    logger.error("❌ Impossible de se connecter à PostgreSQL après %d tentatives", max_retries)
    return False


# ── Logging pipeline ─────────────────────────────────────────────
def log_run(
    flow_name: str,
    etape: str,
    statut: str,
    debut: datetime,
    nb_lignes: int = 0,
    erreur: Optional[str] = None,
    metadata: Optional[dict] = None
) -> int:
    """
    Enregistre une exécution de pipeline dans la table pipeline_runs.

    Args:
        flow_name:  nom du flow Kestra ou script (ex: 'generate_strava').
        etape:      étape dans le flow (ex: 'INSERT strava_activities').
        statut:     'SUCCESS' | 'FAILED' | 'RUNNING' | 'SKIPPED'.
        debut:      datetime de début de l'étape.
        nb_lignes:  nombre de lignes traitées.
        erreur:     message d'erreur si statut == 'FAILED'.
        metadata:   dictionnaire JSON libre d'infos supplémentaires.

    Returns:
        int: id de la ligne insérée dans pipeline_runs.
    """
    import json
    fin = datetime.now()
    duree_ms = int((fin - debut).total_seconds() * 1000)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_runs
                    (flow_name, etape, statut, debut, fin, duree_ms, nb_lignes, erreur, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                flow_name, etape, statut, debut, fin, duree_ms,
                nb_lignes, erreur,
                json.dumps(metadata) if metadata else None
            ))
            run_id = cur.fetchone()[0]

    logger.info(f"📋 Run loggué : {flow_name}/{etape} → {statut} ({nb_lignes} lignes, {duree_ms}ms)")
    return run_id


# ── Lecture paramètres config ────────────────────────────────────
def get_config(cle: str, fallback=None):
    """
    Lit un paramètre métier depuis la table config de PostgreSQL.

    Args:
        cle:      clé de configuration (ex: 'taux_prime').
        fallback: valeur retournée si la clé n'existe pas.

    Returns:
        str: valeur en base, ou fallback si absent.

    Example:
        taux = float(get_config('taux_prime', '0.05'))
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT valeur FROM config WHERE cle = %s", (cle,))
                row = cur.fetchone()
                return row[0] if row else fallback
    except Exception as e:
        logger.warning(f"Impossible de lire config[{cle}] : {e}. Fallback : {fallback}")
        return fallback


def get_all_config() -> dict:
    """
    Retourne tous les paramètres de configuration sous forme de dictionnaire.

    Returns:
        dict: {cle: valeur} pour tous les enregistrements de la table config.
    """
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT cle, valeur FROM config")
                return {row["cle"]: row["valeur"] for row in cur.fetchall()}
    except Exception as e:
        logger.warning(f"Impossible de lire la table config : {e}")
        return {}

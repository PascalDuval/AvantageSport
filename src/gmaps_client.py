"""
Client Google Maps — Round 2

Fournit deux fonctionnalités :
  1. Géocodage d'une adresse → coordonnées GPS
  2. Calcul de distance domicile ↔ bureau

Deux modes de fonctionnement :
  - Mode réel  (GOOGLE_MAPS_API_KEY défini dans .env)
  - Mode mock  (GMAPS_MOCK_MODE=true, ou clé absente)
    → distances pseudo-réalistes déterministes (basées sur hash adresse)
    → utile pour développer/tester sans coûts API

Cache PostgreSQL (table gmaps_cache) :
  → un hash SHA-256 de (adresse + mode_transport) est calculé
  → si le résultat est en cache, pas d'appel API
  → sinon, appel API + mise en cache

Usage:
    client = get_client()
    distance = client.distance_to_bureau("3 rue de la Paix, Montpellier", "walking")
    print(distance)  # ex : 7.3  (km)
"""
import hashlib
import logging
import os
import random
import sys
from pathlib import Path
from typing import Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent))
from config import ADRESSE_BUREAU
from database import get_connection

logger = logging.getLogger(__name__)

# ── Mode de déplacement → mode Google Maps Distance Matrix ───────
MODE_TO_GMAPS = {
    "marche_running":   "walking",
    "velo_trottinette": "bicycling",
    "transports_commun":"transit",
    "vehicule":         "driving",
    # Valeurs brutes au cas où Silver non encore appliqué
    "Marche/running":           "walking",
    "Vélo/Trottinette/Autres":  "bicycling",
    "Transports en commun":     "transit",
    "véhicule thermique/électrique": "driving",
}

# ── Distributions de distances mock (km) par mode ────────────────
# Représentatives d'une population autour de Lattes / Montpellier
MOCK_DISTRIBUTIONS = {
    "walking":   [(0.4, 2), (0.3, 8), (0.2, 20), (0.1, 60)],    # (proba, km_max)
    "bicycling": [(0.3, 10), (0.3, 25), (0.25, 50), (0.15, 100)],
    "transit":   [(0.25, 15), (0.35, 40), (0.3, 80), (0.1, 120)],
    "driving":   [(0.2, 20), (0.35, 50), (0.3, 100), (0.15, 200)],
}


class GmapsClient:
    """
    Client Google Maps avec gestion du cache et mode mock.

    Attributes:
        api_key:   clé API Google Maps (None en mode mock).
        mock_mode: si True, aucun appel réseau (distances simulées).
        bureau:    adresse du bureau (défaut : config.ADRESSE_BUREAU).
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        mock_mode: bool = False,
        bureau: str = ADRESSE_BUREAU,
    ):
        self.api_key = api_key
        self.mock_mode = mock_mode or not api_key
        self.bureau = bureau

        if self.mock_mode:
            logger.info("🗺  GmapsClient en mode MOCK (distances simulées)")
        else:
            logger.info("🗺  GmapsClient en mode RÉEL (API Google Maps)")

    # ── Hash d'adresse ────────────────────────────────────────────
    @staticmethod
    def _hash(adresse_origine: str, adresse_dest: str, mode: str) -> str:
        """
        Calcule un hash SHA-256 de la combinaison adresse+mode.
        Sert de clé de cache (stable, insensible à la casse).

        Args:
            adresse_origine: adresse de départ.
            adresse_dest:    adresse d'arrivée.
            mode:            mode de transport Google Maps.

        Returns:
            str: hash hexadécimal de 64 caractères.
        """
        key = f"{adresse_origine.lower().strip()}|{adresse_dest.lower().strip()}|{mode}"
        return hashlib.sha256(key.encode()).hexdigest()

    # ── Cache PostgreSQL ─────────────────────────────────────────
    def _get_from_cache(self, addr_hash: str) -> Optional[float]:
        """
        Consulte le cache PostgreSQL.

        Args:
            addr_hash: hash SHA-256 de la clé (adresse + mode).

        Returns:
            float: distance en km si en cache, None sinon.
        """
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT distance_km FROM gmaps_cache WHERE adresse_hash = %s",
                        (addr_hash,)
                    )
                    row = cur.fetchone()
                    return float(row[0]) if row else None
        except Exception as e:
            logger.debug(f"Cache miss ou erreur : {e}")
            return None

    def _save_to_cache(
        self,
        addr_hash: str,
        adresse_origine: str,
        adresse_dest: str,
        mode: str,
        distance_km: float,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
    ) -> None:
        """
        Enregistre un résultat dans le cache PostgreSQL.

        Args:
            addr_hash:       hash de la clé.
            adresse_origine: adresse source.
            adresse_dest:    adresse destination.
            mode:            mode de transport.
            distance_km:     distance calculée (km).
            lat:             latitude de l'adresse source.
            lng:             longitude de l'adresse source.
        """
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO gmaps_cache
                            (adresse_hash, adresse_origine, adresse_dest,
                             distance_km, lat_origine, lng_origine, mode_transport)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (adresse_hash) DO NOTHING
                    """, (addr_hash, adresse_origine, adresse_dest,
                          distance_km, lat, lng, mode))
        except Exception as e:
            logger.warning(f"Impossible de sauvegarder en cache : {e}")

    # ── Distance mock ────────────────────────────────────────────
    def _mock_distance(self, adresse_origine: str, mode: str) -> float:
        """
        Génère une distance pseudo-réaliste de façon déterministe.

        Utilise le hash de l'adresse comme seed aléatoire pour que
        la même adresse donne toujours la même distance (reproductible).

        Args:
            adresse_origine: adresse du salarié.
            mode:            mode de transport Google Maps.

        Returns:
            float: distance simulée en km (arrondie à 1 décimale).
        """
        # Seed déterministe basé sur l'adresse
        seed = int(hashlib.md5(adresse_origine.lower().encode()).hexdigest(), 16) % (2**32)
        rng = random.Random(seed)

        distribution = MOCK_DISTRIBUTIONS.get(mode, MOCK_DISTRIBUTIONS["driving"])

        # Tirage de la tranche selon les probabilités
        draw = rng.random()
        cumul = 0.0
        for proba, km_max in distribution:
            cumul += proba
            if draw <= cumul:
                prev_max = 0 if distribution.index((proba, km_max)) == 0 \
                    else distribution[distribution.index((proba, km_max)) - 1][1]
                km = rng.uniform(prev_max, km_max)
                return round(km, 1)

        return round(rng.uniform(50, 150), 1)  # fallback

    # ── Géocodage ────────────────────────────────────────────────
    def geocode(self, adresse: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Retourne les coordonnées GPS d'une adresse.

        En mode mock, retourne des coordonnées fictives cohérentes
        autour de Montpellier (lat ~43.6, lng ~3.9).

        Args:
            adresse: adresse textuelle à géocoder.

        Returns:
            Tuple (lat, lng) ou (None, None) si échec.
        """
        if self.mock_mode:
            seed = int(hashlib.md5(adresse.lower().encode()).hexdigest(), 16) % (2**32)
            rng = random.Random(seed)
            lat = round(43.3 + rng.uniform(0, 0.8), 6)
            lng = round(3.5 + rng.uniform(0, 1.2), 6)
            return lat, lng

        try:
            import googlemaps
            gmaps = googlemaps.Client(key=self.api_key)
            result = gmaps.geocode(adresse)
            if result:
                loc = result[0]["geometry"]["location"]
                return loc["lat"], loc["lng"]
        except Exception as e:
            logger.error(f"Erreur géocodage '{adresse}' : {e}")
        return None, None

    # ── Distance domicile → bureau ───────────────────────────────
    def distance_to_bureau(
        self,
        adresse_origine: str,
        mode_deplacement: str,
    ) -> Optional[float]:
        """
        Calcule la distance entre une adresse et le bureau.

        Ordre de priorité :
        1. Cache PostgreSQL (gratuit, instantané)
        2. API Google Maps Distance Matrix (si mode réel)
        3. Distance mock (si mode mock ou erreur API)

        Args:
            adresse_origine:   adresse domicile du salarié.
            mode_deplacement:  mode normalisé (ex: 'marche_running')
                               ou brut (ex: 'Marche/running').

        Returns:
            float: distance en km, ou None si impossible à calculer.
        """
        if not adresse_origine or str(adresse_origine).strip() in ("", "nan", "None"):
            logger.warning("Adresse vide — distance non calculée")
            return None

        # Résoudre le mode vers l'équivalent Google Maps
        gmaps_mode = MODE_TO_GMAPS.get(
            mode_deplacement,
            MODE_TO_GMAPS.get("driving")  # fallback
        )

        addr_hash = self._hash(adresse_origine, self.bureau, gmaps_mode)

        # 1. Cache
        cached = self._get_from_cache(addr_hash)
        if cached is not None:
            logger.debug(f"Cache hit : {adresse_origine[:30]}… → {cached} km")
            return cached

        # 2. API réelle ou mock
        if self.mock_mode:
            distance_km = self._mock_distance(adresse_origine, gmaps_mode)
            lat, lng = self.geocode(adresse_origine)
            logger.debug(f"MOCK : {adresse_origine[:30]}… → {distance_km} km ({gmaps_mode})")
        else:
            distance_km = self._real_distance(adresse_origine, gmaps_mode)
            lat, lng = self.geocode(adresse_origine)
            if distance_km is None:
                # Fallback mock si API échoue
                distance_km = self._mock_distance(adresse_origine, gmaps_mode)
                logger.warning(f"API KO → fallback mock : {distance_km} km")

        # 3. Mise en cache
        if distance_km is not None:
            self._save_to_cache(
                addr_hash, adresse_origine, self.bureau,
                gmaps_mode, distance_km, lat, lng
            )

        return distance_km

    def _real_distance(self, adresse_origine: str, gmaps_mode: str) -> Optional[float]:
        """
        Appelle l'API Google Maps Distance Matrix.

        Args:
            adresse_origine: adresse de départ.
            gmaps_mode:      mode transport Google Maps.

        Returns:
            float: distance en km, ou None si erreur.
        """
        try:
            import googlemaps
            gmaps = googlemaps.Client(key=self.api_key)
            result = gmaps.distance_matrix(
                origins=[adresse_origine],
                destinations=[self.bureau],
                mode=gmaps_mode,
                language="fr",
            )
            rows = result.get("rows", [])
            if rows and rows[0].get("elements"):
                elem = rows[0]["elements"][0]
                if elem.get("status") == "OK":
                    meters = elem["distance"]["value"]
                    return round(meters / 1000, 2)
        except Exception as e:
            logger.error(f"Erreur Distance Matrix : {e}")
        return None

    def batch_distances(
        self,
        employees: list[dict],
        mode_col: str = "mode_deplacement",
        addr_col: str = "adresse_domicile",
    ) -> list[dict]:
        """
        Calcule les distances pour une liste de salariés.

        Enrichit chaque dict avec les clés 'distance_km', 'lat', 'lng'.
        Affiche une barre de progression.

        Args:
            employees: liste de dicts avec au moins addr_col et mode_col.
            mode_col:  clé du mode de déplacement dans chaque dict.
            addr_col:  clé de l'adresse dans chaque dict.

        Returns:
            Liste enrichie avec 'distance_km', 'lat', 'lng'.
        """
        total = len(employees)
        logger.info(f"🗺  Calcul distances pour {total} salariés...")

        results = []
        for i, emp in enumerate(employees, 1):
            addr = str(emp.get(addr_col, ""))
            mode = str(emp.get(mode_col, "driving"))
            dist = self.distance_to_bureau(addr, mode)
            lat, lng = self.geocode(addr) if dist is not None else (None, None)

            results.append({
                **emp,
                "distance_km": dist,
                "lat":         lat,
                "lng":         lng,
            })

            if i % 20 == 0 or i == total:
                logger.info(f"   {i}/{total} distances calculées")

        cached_count = sum(1 for r in results if r.get("distance_km") is not None)
        logger.info(f"✅ {cached_count}/{total} distances obtenues")
        return results


# ── Fabrique ─────────────────────────────────────────────────────
def get_client() -> GmapsClient:
    """
    Retourne un client GmapsClient configuré depuis les variables d'env.

    - Si GOOGLE_MAPS_API_KEY est définie et GMAPS_MOCK_MODE != 'true' → mode réel
    - Sinon → mode mock (aucun appel API, distances simulées)

    Returns:
        GmapsClient: instance prête à l'emploi.
    """
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    mock_mode = os.getenv("GMAPS_MOCK_MODE", "true").lower() == "true"

    if not api_key:
        logger.info("💡 GOOGLE_MAPS_API_KEY non définie → mode mock activé")
        mock_mode = True

    return GmapsClient(api_key=api_key, mock_mode=mock_mode)

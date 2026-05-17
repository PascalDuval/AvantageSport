-- =============================================================
-- POC Avantages Sportifs — Schéma PostgreSQL complet
-- Créé automatiquement au 1er démarrage du container
-- =============================================================

-- Extensions utiles
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================
-- TABLE 1 : raw_rh
-- Données RH brutes telles que lues depuis le XLSX
-- (IDs float, dates non converties, valeurs brutes)
-- =============================================================
CREATE TABLE IF NOT EXISTS raw_rh (
    id              SERIAL PRIMARY KEY,
    id_salarie      VARCHAR(20),           -- sera '59019.0' avant normalisation
    nom             VARCHAR(100),
    prenom          VARCHAR(100),
    date_naissance  VARCHAR(50),           -- brut : '1990-07-06 00:00:00'
    bu              VARCHAR(100),
    date_embauche   VARCHAR(50),           -- brut
    salaire_brut    VARCHAR(50),           -- brut : '30940.0'
    type_contrat    VARCHAR(50),
    nb_jours_cp     VARCHAR(20),
    adresse_domicile TEXT,
    mode_deplacement VARCHAR(100),         -- valeur brute du fichier
    inserted_at     TIMESTAMP DEFAULT NOW(),
    source_file     VARCHAR(255)
);

-- =============================================================
-- TABLE 2 : raw_sport
-- Déclarations sportives brutes depuis XLSX
-- =============================================================
CREATE TABLE IF NOT EXISTS raw_sport (
    id              SERIAL PRIMARY KEY,
    id_salarie      VARCHAR(20),
    pratique_sport  VARCHAR(100),          -- inclut 'Runing' (typo Round 2)
    inserted_at     TIMESTAMP DEFAULT NOW(),
    source_file     VARCHAR(255)
);

-- =============================================================
-- TABLE 3 : strava_activities
-- Activités générées (générateur Strava-like / Round 1)
-- puis réelles (API Strava / Production)
-- =============================================================
CREATE TABLE IF NOT EXISTS strava_activities (
    id              SERIAL PRIMARY KEY,
    employee_id     VARCHAR(20) NOT NULL,  -- référence id_salarie
    date_debut      TIMESTAMP NOT NULL,
    date_fin        TIMESTAMP,
    sport_type      VARCHAR(100) NOT NULL,
    distance_m      FLOAT,                 -- NULL si non pertinent (escalade, yoga...)
    duree_s         INTEGER,               -- durée en secondes
    commentaire     TEXT,
    source          VARCHAR(50) DEFAULT 'generator',  -- 'generator' | 'strava_api' | 'manual'
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_strava_employee ON strava_activities(employee_id);
CREATE INDEX IF NOT EXISTS idx_strava_date ON strava_activities(date_debut);
CREATE INDEX IF NOT EXISTS idx_strava_sport ON strava_activities(sport_type);

-- =============================================================
-- TABLE 4 : employees
-- Données RH nettoyées et normalisées (couche Silver/Gold)
-- Peuplée par l'ETL Round 3
-- =============================================================
CREATE TABLE IF NOT EXISTS employees (
    id              VARCHAR(20) PRIMARY KEY,  -- ex : '59019'
    nom             VARCHAR(100),
    prenom          VARCHAR(100),
    date_naissance  DATE,
    bu              VARCHAR(100),
    date_embauche   DATE,
    salaire_brut    NUMERIC(12,2),
    type_contrat    VARCHAR(50),
    nb_jours_cp     INTEGER,
    adresse_domicile TEXT,
    mode_deplacement VARCHAR(50),             -- enum normalisé
    sport_declare   VARCHAR(100),             -- sport normalisé (Runing → Running)
    coords_lat      FLOAT,                    -- géocodage Google Maps
    coords_lng      FLOAT,
    distance_bureau_km FLOAT,                 -- calculée via Google Maps API
    eligible_prime  BOOLEAN,                  -- Round 3
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- =============================================================
-- TABLE 5 : avantages_calcules
-- Résultats Gold : primes et journées bien-être calculés
-- =============================================================
CREATE TABLE IF NOT EXISTS avantages_calcules (
    id              SERIAL PRIMARY KEY,
    employee_id     VARCHAR(20) NOT NULL,
    montant_prime   NUMERIC(12,2),
    eligible_prime  BOOLEAN DEFAULT FALSE,
    eligible_jours_be BOOLEAN DEFAULT FALSE,
    nb_activites_12m INTEGER DEFAULT 0,
    nb_jours_bienetre INTEGER DEFAULT 0,
    date_calcul     DATE DEFAULT CURRENT_DATE,
    params_version  VARCHAR(50),              -- ex : 'v1.0_taux5pct'
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_avantages_employee ON avantages_calcules(employee_id);
CREATE INDEX IF NOT EXISTS idx_avantages_date ON avantages_calcules(date_calcul);

-- =============================================================
-- TABLE 6 : config
-- Paramètres métier modifiables sans re-déploiement
-- Permet le recalcul historique Power BI
-- =============================================================
CREATE TABLE IF NOT EXISTS config (
    cle             VARCHAR(100) PRIMARY KEY,
    valeur          VARCHAR(255) NOT NULL,
    description     TEXT,
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Valeurs par défaut
INSERT INTO config (cle, valeur, description) VALUES
    ('taux_prime',          '0.05',                                     'Taux de la prime sportive (5%)'),
    ('seuil_marche_km',     '15.0',                                     'Distance max Marche/Running pour être éligible'),
    ('seuil_velo_km',       '25.0',                                     'Distance max Vélo/Trottinette pour être éligible'),
    ('min_activites_be',    '15',                                        'Nombre minimum d''activités pour les journées bien-être'),
    ('nb_jours_bienetre',   '5',                                         'Nombre de jours bien-être accordés'),
    ('adresse_bureau',      '1362 Av. des Platanes, 34970 Lattes',      'Adresse de l''entreprise'),
    ('params_version',      'v1.0_taux5pct',                            'Version des paramètres en cours'),
    ('fenetre_activites_j', '365',                                       'Fenêtre de comptage des activités (jours)')
ON CONFLICT (cle) DO NOTHING;

-- =============================================================
-- TABLE 7 : gmaps_cache
-- Cache des appels Google Maps API (géocodage + distances)
-- Évite les appels répétés = économie de coûts API
-- =============================================================
CREATE TABLE IF NOT EXISTS gmaps_cache (
    id              SERIAL PRIMARY KEY,
    adresse_hash    VARCHAR(64) UNIQUE NOT NULL,  -- SHA256 de l'adresse normalisée
    adresse_origine TEXT NOT NULL,
    adresse_dest    TEXT NOT NULL,
    distance_km     FLOAT,
    lat_origine     FLOAT,
    lng_origine     FLOAT,
    mode_transport  VARCHAR(50),                   -- 'walking', 'bicycling', 'driving'
    cached_at       TIMESTAMP DEFAULT NOW()
);

-- =============================================================
-- TABLE 8 : pipeline_runs
-- Logs d'exécution Kestra + scripts manuels
-- =============================================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              SERIAL PRIMARY KEY,
    run_id          UUID DEFAULT uuid_generate_v4(),
    flow_name       VARCHAR(100) NOT NULL,         -- ex : 'generate_strava', 'etl_silver'
    etape           VARCHAR(100),
    statut          VARCHAR(20) CHECK (statut IN ('RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED')),
    debut           TIMESTAMP DEFAULT NOW(),
    fin             TIMESTAMP,
    duree_ms        INTEGER,
    nb_lignes       INTEGER,                       -- lignes traitées
    erreur          TEXT,
    metadata        JSONB                          -- infos supplémentaires
);

CREATE INDEX IF NOT EXISTS idx_pipeline_flow ON pipeline_runs(flow_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_statut ON pipeline_runs(statut);

-- =============================================================
-- TABLE 9 : data_quality_results
-- Résultats Great Expectations (Round 3+)
-- =============================================================
CREATE TABLE IF NOT EXISTS data_quality_results (
    id              SERIAL PRIMARY KEY,
    run_id          UUID,
    suite_name      VARCHAR(100),
    regle           VARCHAR(200),
    table_cible     VARCHAR(100),
    colonne         VARCHAR(100),
    resultat        BOOLEAN,
    severite        VARCHAR(20) CHECK (severite IN ('BLOQUANT', 'WARNING')),
    detail          TEXT,
    run_at          TIMESTAMP DEFAULT NOW()
);

-- =============================================================
-- Vues utiles
-- =============================================================

-- Vue : résumé des avantages par BU (utilisée par Power BI)
CREATE OR REPLACE VIEW v_avantages_bu AS
SELECT
    e.bu,
    COUNT(*) AS nb_salaries,
    SUM(CASE WHEN a.eligible_prime THEN 1 ELSE 0 END) AS nb_eligible_prime,
    SUM(a.montant_prime) AS total_primes,
    SUM(CASE WHEN a.eligible_jours_be THEN 1 ELSE 0 END) AS nb_eligible_be
FROM employees e
LEFT JOIN avantages_calcules a ON a.employee_id = e.id
GROUP BY e.bu;

-- Vue : statistiques activités par sport
CREATE OR REPLACE VIEW v_stats_sports AS
SELECT
    sport_type,
    COUNT(*) AS nb_activites,
    COUNT(DISTINCT employee_id) AS nb_athletes,
    ROUND(AVG(distance_m / 1000.0)::NUMERIC, 2) AS distance_moy_km,
    ROUND(AVG(duree_s / 60.0)::NUMERIC, 1) AS duree_moy_min
FROM strava_activities
WHERE distance_m IS NOT NULL
GROUP BY sport_type
ORDER BY nb_activites DESC;

-- Message de confirmation
DO $$
BEGIN
    RAISE NOTICE '✅ Schéma POC Avantages Sportifs créé avec succès — 9 tables + 2 vues';
END $$;

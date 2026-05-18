-- =============================================================
-- SQL Tableau — POC Avantages Sportifs
-- Ces requêtes alimentent export_tableau.py et peuvent être
-- copiées directement dans Tableau Desktop (connexion PostgreSQL)
-- =============================================================

-- ── CONNEXION TABLEAU DESKTOP ────────────────────────────────
-- Se connecter → PostgreSQL
-- Serveur   : localhost
-- Port      : 5432
-- Base      : poc_sport
-- User      : admin | Mot de passe : admin123
-- (Pas de driver ODBC requis — connecteur natif Tableau)
--
-- Alternative : lire les fichiers .hyper via
--   python src/export_tableau.py --format hyper
--   → Tableau Desktop → Se connecter → Fichiers supplémentaires → *.hyper


-- ════════════════════════════════════════════════════════════
-- Feuille 1 : Vue Globale — KPI synthèse par BU
-- ════════════════════════════════════════════════════════════
-- Visuels recommandés : cartes KPI, barres groupées par BU, donut
-- Filtres Tableau : Params_Version, date_calcul
-- Mesures calculées : % éligibles = SUM(nb_eligible_prime) / SUM(nb_salaries)
SELECT
    e.bu                                                          AS bu,
    COUNT(*)                                                      AS nb_salaries,
    SUM(CASE WHEN a.eligible_prime    THEN 1 ELSE 0 END)         AS nb_eligible_prime,
    ROUND(SUM(a.montant_prime)::NUMERIC, 2)                      AS cout_prime_euros,
    SUM(CASE WHEN a.eligible_jours_be THEN 1 ELSE 0 END)        AS nb_eligible_be,
    SUM(a.nb_jours_bienetre)                                      AS total_jours_be,
    ROUND(AVG(CASE WHEN a.eligible_prime THEN a.montant_prime END)::NUMERIC, 2) AS prime_moy_euros,
    MAX(a.params_version)                                         AS params_version,
    MAX(a.date_calcul)                                            AS date_calcul
FROM avantages_calcules a
JOIN employees e ON e.id = a.employee_id
GROUP BY e.bu
ORDER BY cout_prime_euros DESC;


-- ════════════════════════════════════════════════════════════
-- Feuille 2 : Primes Sportives — liste et détail par salarié
-- ════════════════════════════════════════════════════════════
-- Visuels : nuage de points (distance vs prime), tableau détaillé
-- Filtres : eligible_prime, bu, mode_deplacement
-- Paramètre Tableau : Taux Prime Simulation → Champ calculé : salaire_brut * [Taux]
SELECT
    e.id                                                          AS id_salarie,
    e.nom                                                         AS nom,
    e.prenom                                                      AS prenom,
    e.bu                                                          AS bu,
    e.type_contrat                                                AS type_contrat,
    e.mode_deplacement                                            AS mode_deplacement,
    ROUND(e.distance_bureau_km::NUMERIC, 2)                      AS distance_bureau_km,
    e.salaire_brut                                                AS salaire_brut,
    a.eligible_prime                                              AS eligible_prime,
    ROUND(a.montant_prime::NUMERIC, 2)                           AS montant_prime_euros,
    a.nb_activites_12m                                            AS nb_activites_12m,
    a.eligible_jours_be                                           AS eligible_jours_be,
    a.nb_jours_bienetre                                           AS nb_jours_be,
    a.params_version                                              AS params_version,
    CASE e.mode_deplacement
        WHEN 'marche_running'    THEN 'Marche / Running'
        WHEN 'velo_trottinette'  THEN 'Vélo / Trottinette'
        WHEN 'transports_commun' THEN 'Transports en commun'
        WHEN 'vehicule'          THEN 'Véhicule'
        ELSE e.mode_deplacement
    END                                                           AS mode_label
FROM avantages_calcules a
JOIN employees e ON e.id = a.employee_id
ORDER BY a.montant_prime DESC, e.nom;


-- ════════════════════════════════════════════════════════════
-- Feuille 3 : Journées Bien-être — activités et éligibilité
-- ════════════════════════════════════════════════════════════
-- Visuels : histogramme nb_activites_12m, ligne de référence à 15,
--           treemap sports, jauge éligibilité
-- Filtres : tranche_activite, sport_declare, bu
SELECT
    e.id                                                          AS id_salarie,
    e.nom                                                         AS nom,
    e.prenom                                                      AS prenom,
    e.bu                                                          AS bu,
    e.sport_declare                                               AS sport_declare,
    a.nb_activites_12m                                            AS nb_activites_12m,
    a.eligible_jours_be                                           AS eligible_jours_be,
    a.nb_jours_bienetre                                           AS nb_jours_be,
    CASE
        WHEN a.nb_activites_12m = 0   THEN '0 — Aucune activité'
        WHEN a.nb_activites_12m < 5   THEN '1-4 — Peu actif'
        WHEN a.nb_activites_12m < 15  THEN '5-14 — Occasionnel'
        WHEN a.nb_activites_12m < 30  THEN '15-29 — Actif (éligible BE)'
        ELSE                               '30+ — Très actif'
    END                                                           AS tranche_activite,
    (a.nb_activites_12m >= 15)                                   AS depasse_seuil_15,
    a.params_version                                              AS params_version
FROM avantages_calcules a
JOIN employees e ON e.id = a.employee_id
ORDER BY a.nb_activites_12m DESC;


-- ════════════════════════════════════════════════════════════
-- Feuille 4 : Activités Sportives — volumes et tendances
-- ════════════════════════════════════════════════════════════
-- Visuels : courbe temporelle (mois × nb_activites), barres par sport,
--           carte de chaleur BU × mois
-- Filtres : sport, annee, bu, source
SELECT
    sa.employee_id                                                AS id_salarie,
    e.nom                                                         AS nom,
    e.prenom                                                      AS prenom,
    e.bu                                                          AS bu,
    sa.sport_type                                                 AS sport,
    DATE_TRUNC('month', sa.date_debut)::date                     AS mois,
    EXTRACT(YEAR  FROM sa.date_debut)::int                       AS annee,
    EXTRACT(MONTH FROM sa.date_debut)::int                       AS num_mois,
    TO_CHAR(sa.date_debut, 'Mon YYYY')                           AS label_mois,
    COUNT(*)                                                      AS nb_activites,
    ROUND(AVG(sa.distance_m / 1000.0)::NUMERIC, 2)              AS distance_moy_km,
    ROUND(AVG(sa.duree_s    / 60.0  )::NUMERIC, 1)              AS duree_moy_min,
    ROUND(SUM(sa.distance_m / 1000.0)::NUMERIC, 2)              AS distance_tot_km,
    sa.source                                                     AS source
FROM strava_activities sa
LEFT JOIN employees e ON e.id = sa.employee_id
GROUP BY
    sa.employee_id, e.nom, e.prenom, e.bu, sa.sport_type,
    DATE_TRUNC('month', sa.date_debut),
    EXTRACT(YEAR FROM sa.date_debut), EXTRACT(MONTH FROM sa.date_debut),
    TO_CHAR(sa.date_debut, 'Mon YYYY'), sa.source
ORDER BY mois, sport;


-- ════════════════════════════════════════════════════════════
-- Feuille 5 : Anomalies & Qualité — contrôles SODA
-- ════════════════════════════════════════════════════════════
-- Visuels : tableau texte avec mise en forme conditionnelle (vert/rouge),
--           donut OK/KO, courbe historique des runs
-- Filtres : dernier_run, severite, table_cible
WITH last_run AS (
    SELECT MAX(run_at) AS last_run_at
    FROM data_quality_results
)
SELECT
    dqr.run_id                                                    AS run_id,
    dqr.run_at                                                    AS date_run,
    dqr.suite_name                                                AS suite,
    dqr.regle                                                     AS regle,
    dqr.table_cible                                               AS table_cible,
    dqr.colonne                                                   AS colonne,
    dqr.severite                                                  AS severite,
    dqr.resultat                                                  AS resultat_ok,
    CASE WHEN dqr.resultat THEN 'OK'
         WHEN dqr.severite = 'BLOQUANT' THEN 'BLOQUANT'
         ELSE 'WARNING' END                                       AS statut,
    dqr.detail                                                    AS detail,
    (dqr.run_at = lr.last_run_at)                                AS dernier_run
FROM data_quality_results dqr
CROSS JOIN last_run lr
ORDER BY dqr.run_at DESC, dqr.severite DESC;


-- ════════════════════════════════════════════════════════════
-- Paramètres config (filtres et paramètres Tableau)
-- ════════════════════════════════════════════════════════════
-- Utilisé avec le Paramètre Tableau "Taux Prime Simulation"
-- Champ calculé : [salaire_brut] * [Taux Prime Simulation]
SELECT
    cle         AS parametre,
    valeur      AS valeur,
    description AS description,
    updated_at  AS mis_a_jour
FROM config
ORDER BY cle;


-- ════════════════════════════════════════════════════════════
-- Monitoring pipeline (onglet optionnel dans le dashboard)
-- ════════════════════════════════════════════════════════════
SELECT
    id          AS id,
    flow_name   AS flow,
    etape       AS etape,
    statut      AS statut,
    debut       AS debut,
    fin         AS fin,
    duree_ms    AS duree_ms,
    nb_lignes   AS nb_lignes,
    erreur      AS erreur
FROM pipeline_runs
ORDER BY debut DESC
LIMIT 200;

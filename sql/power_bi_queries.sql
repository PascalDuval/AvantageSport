-- =============================================================
-- SQL Power BI — POC Avantages Sportifs
-- Ces requêtes sont utilisées dans export_power_bi.py
-- et peuvent être copiées directement dans Power BI
-- (Obtenir données → Base de données PostgreSQL)
-- =============================================================

-- ── CONNEXION ODBC Power BI ──────────────────────────────────
-- Server   : localhost
-- Database : poc_sport
-- Driver   : PostgreSQL ODBC (psqlODBC — télécharger sur postgresql.org)
-- User     : admin | Password : admin123
-- Ou utiliser DuckDB ODBC pour lire les fichiers Delta Lake directement


-- ════════════════════════════════════════════════════════════
-- Page 1 : Vue Globale — KPI synthèse par BU
-- ════════════════════════════════════════════════════════════
-- Indicateurs : coût total primes, nb éligibles, jours BE accordés
-- Slice par : BU, params_version
-- Visuels : carte KPI, graphique barres empilées, matrice
SELECT
    e.bu                                                          AS BU,
    COUNT(*)                                                      AS Nb_Salaries,
    SUM(CASE WHEN a.eligible_prime    THEN 1 ELSE 0 END)         AS Nb_Eligible_Prime,
    ROUND(SUM(a.montant_prime)::NUMERIC, 2)                      AS Cout_Prime_Euros,
    SUM(CASE WHEN a.eligible_jours_be THEN 1 ELSE 0 END)        AS Nb_Eligible_BE,
    SUM(a.nb_jours_bienetre)                                      AS Total_Jours_BE,
    ROUND(AVG(CASE WHEN a.eligible_prime THEN a.montant_prime END)::NUMERIC, 2) AS Prime_Moy_Euros,
    MAX(a.params_version)                                         AS Params_Version,
    MAX(a.date_calcul)                                            AS Date_Calcul
FROM avantages_calcules a
JOIN employees e ON e.id = a.employee_id
GROUP BY e.bu
ORDER BY Cout_Prime_Euros DESC;


-- ════════════════════════════════════════════════════════════
-- Page 2 : Primes Sportives — liste et détail par salarié
-- ════════════════════════════════════════════════════════════
-- Indicateurs : montant prime, mode de déplacement, distance
-- Filtres : éligibles seulement, BU, mode déplacement
-- Visuels : tableau détaillé, donut mode déplacement, scatter distance/prime
SELECT
    e.id                                                          AS ID_Salarie,
    e.nom                                                         AS Nom,
    e.prenom                                                      AS Prenom,
    e.bu                                                          AS BU,
    e.type_contrat                                                AS Type_Contrat,
    e.mode_deplacement                                            AS Mode_Deplacement,
    ROUND(e.distance_bureau_km::NUMERIC, 2)                      AS Distance_Bureau_Km,
    e.salaire_brut                                                AS Salaire_Brut,
    a.eligible_prime                                              AS Eligible_Prime,
    ROUND(a.montant_prime::NUMERIC, 2)                           AS Montant_Prime_Euros,
    a.nb_activites_12m                                            AS Nb_Activites_12m,
    a.eligible_jours_be                                           AS Eligible_Jours_BE,
    a.nb_jours_bienetre                                           AS Nb_Jours_BE,
    a.params_version                                              AS Params_Version,
    CASE e.mode_deplacement
        WHEN 'marche_running'   THEN '🚶 Marche / Running'
        WHEN 'velo_trottinette' THEN '🚴 Vélo / Trottinette'
        WHEN 'transports_commun'THEN '🚇 Transports en commun'
        WHEN 'vehicule'         THEN '🚗 Véhicule'
        ELSE e.mode_deplacement
    END                                                           AS Mode_Label
FROM avantages_calcules a
JOIN employees e ON e.id = a.employee_id
ORDER BY a.montant_prime DESC, e.nom;


-- ════════════════════════════════════════════════════════════
-- Page 3 : Journées Bien-être — activités et éligibilité
-- ════════════════════════════════════════════════════════════
-- Indicateurs : nb activités, éligibilité BE, répartition par sport
-- Filtres : sport, BU, tranche d'activité
-- Visuels : histogramme activités, treemap sports, jauge seuil 15
SELECT
    e.id                                                          AS ID_Salarie,
    e.nom                                                         AS Nom,
    e.prenom                                                      AS Prenom,
    e.bu                                                          AS BU,
    e.sport_declare                                               AS Sport_Declare,
    a.nb_activites_12m                                            AS Nb_Activites_12m,
    a.eligible_jours_be                                           AS Eligible_Jours_BE,
    a.nb_jours_bienetre                                           AS Nb_Jours_BE,
    CASE
        WHEN a.nb_activites_12m = 0       THEN '0 — Aucune activité'
        WHEN a.nb_activites_12m < 5        THEN '1-4 — Peu actif'
        WHEN a.nb_activites_12m < 15       THEN '5-14 — Occasionnel'
        WHEN a.nb_activites_12m < 30       THEN '15-29 — ✅ Actif (éligible BE)'
        ELSE                                    '30+ — Très actif'
    END                                                           AS Tranche_Activite,
    (a.nb_activites_12m >= 15)                                   AS Depasse_Seuil_15,
    a.params_version                                              AS Params_Version
FROM avantages_calcules a
JOIN employees e ON e.id = a.employee_id
ORDER BY a.nb_activites_12m DESC;


-- ════════════════════════════════════════════════════════════
-- Page 4 : Activités Sportives — volumes et tendances
-- ════════════════════════════════════════════════════════════
-- Indicateurs : activités/mois, top sports, distances moyennes
-- Filtres : sport, mois, BU
-- Visuels : courbe temporelle, barres sports, carte chaleur mois×sport
SELECT
    sa.employee_id                                                AS ID_Salarie,
    e.nom                                                         AS Nom,
    e.prenom                                                      AS Prenom,
    e.bu                                                          AS BU,
    sa.sport_type                                                 AS Sport,
    DATE_TRUNC('month', sa.date_debut)::date                     AS Mois,
    EXTRACT(YEAR  FROM sa.date_debut)::int                       AS Annee,
    EXTRACT(MONTH FROM sa.date_debut)::int                       AS Num_Mois,
    TO_CHAR(sa.date_debut, 'Mon YYYY')                           AS Label_Mois,
    COUNT(*)                                                      AS Nb_Activites,
    ROUND(AVG(sa.distance_m / 1000.0)::NUMERIC, 2)              AS Distance_Moy_Km,
    ROUND(AVG(sa.duree_s    / 60.0  )::NUMERIC, 1)              AS Duree_Moy_Min,
    ROUND(SUM(sa.distance_m / 1000.0)::NUMERIC, 2)              AS Distance_Tot_Km,
    sa.source                                                     AS Source
FROM strava_activities sa
LEFT JOIN employees e ON e.id = sa.employee_id
GROUP BY
    sa.employee_id, e.nom, e.prenom, e.bu, sa.sport_type,
    DATE_TRUNC('month', sa.date_debut),
    EXTRACT(YEAR FROM sa.date_debut), EXTRACT(MONTH FROM sa.date_debut),
    TO_CHAR(sa.date_debut, 'Mon YYYY'), sa.source
ORDER BY Mois, Sport;


-- ════════════════════════════════════════════════════════════
-- Page 5 : Anomalies & Qualité — contrôles data quality
-- ════════════════════════════════════════════════════════════
-- Indicateurs : règles OK/KO, taux d'anomalie, historique runs
-- Filtres : sévérité, table, dernier run
-- Visuels : tableau règles, donut OK/KO, courbe historique
WITH last_run AS (
    SELECT MAX(run_at) AS last_run_at
    FROM data_quality_results
)
SELECT
    dqr.run_id                                                    AS Run_ID,
    dqr.run_at                                                    AS Date_Run,
    dqr.suite_name                                                AS Suite,
    dqr.regle                                                     AS Regle,
    dqr.table_cible                                               AS Table_Cible,
    dqr.colonne                                                   AS Colonne,
    dqr.severite                                                  AS Severite,
    dqr.resultat                                                  AS Resultat_OK,
    CASE WHEN dqr.resultat THEN 'OK'
         WHEN dqr.severite = 'BLOQUANT' THEN 'BLOQUANT'
         ELSE 'WARNING' END                                       AS Statut,
    dqr.detail                                                    AS Detail,
    (dqr.run_at = lr.last_run_at)                                AS Dernier_Run
FROM data_quality_results dqr
CROSS JOIN last_run lr
ORDER BY dqr.run_at DESC, dqr.severite DESC;


-- ════════════════════════════════════════════════════════════
-- Table de paramètres (pour slicers What-If Power BI)
-- ════════════════════════════════════════════════════════════
SELECT
    cle         AS Parametre,
    valeur      AS Valeur,
    description AS Description,
    updated_at  AS Mis_A_Jour
FROM config
ORDER BY cle;


-- ════════════════════════════════════════════════════════════
-- Monitoring pipeline (onglet optionnel dans le rapport PBI)
-- ════════════════════════════════════════════════════════════
SELECT
    id          AS ID,
    flow_name   AS Flow,
    etape       AS Etape,
    statut      AS Statut,
    debut       AS Debut,
    fin         AS Fin,
    duree_ms    AS Duree_Ms,
    nb_lignes   AS Nb_Lignes,
    erreur      AS Erreur
FROM pipeline_runs
ORDER BY debut DESC
LIMIT 200;

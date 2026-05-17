# POC Avantages Sportifs — Documentation générale

> Pipeline de données end-to-end : ingestion XLSX → normalisation → distances Google Maps → primes sportives → Delta Lake → orchestration Kestra → notifications Slack temps réel → Power BI

---

## Sommaire

1. [Objectif du présent POC](#1-objectif-du-présent-poc)
2. [Architecture technique du POC](#2-architecture-technique-du-poc)
3. [Environnement et démarrage](#3-environnement-et-démarrage)
4. [Round 1 — Infra + Simulation Strava-like](#4-round-1--infra--simulation-strava-like)
5. [Round 2 — Ingestion XLSX + ETL Silver + Google Maps](#5-round-2--ingestion-xlsx--etl-silver--google-maps)
6. [Round 3 — Quality Check + ETL Gold](#6-round-3--quality-check--etl-gold)
7. [Round 4 — Orchestration Kestra + Flask + Notifications Slack](#7-round-4--orchestration-kestra--flask--notifications-slack)
8. [Round 5 — Power BI + Export CSV + Clôture projet](#8-round-5--power-bi--export-csv--clôture-projet)
9. [Schéma de base de données](#9-schéma-de-base-de-données)
10. [Architecture Delta Lake](#10-architecture-delta-lake)
11. [Évolution vers Great Expectations](#11-évolution-vers-great-expectations)
12. [Dépannage](#12-dépannage)

---

## 1. Objectif du présent POC

Ce POC a été initié à la demande de Juliette, Co-Fondatrice de Sport Data Solution. L'email de cadrage initial se trouve dans [documentation/mailjuliette.txt](documentation/mailjuliette.txt) ; la note de cadrage formelle complète est disponible dans [documentation/Note+de+cadrage+_+POC+Avantages+Sportif.pdf](documentation/Note+de+cadrage+_+POC+Avantages+Sportif.pdf).

**Contexte** : après un premier système de récompense des actions environnementales, l'entreprise souhaite maintenant encourager la pratique sportive des collaborateurs via deux avantages :

- **Prime de 5 %** du salaire brut pour les salariés venant au bureau à pied ou à vélo, dont le domicile est à moins de 15 km (marche/running) ou 25 km (vélo/trottinette). L'éligibilité est basée sur le déclaratif salarié, contrôlé via l'API Google Maps.
- **5 journées bien-être** pour les salariés ayant réalisé au moins 15 activités sportives sur les 12 derniers mois.

Une notification Slack est automatiquement envoyée dans le channel dédié à chaque activité physique enregistrée, pour favoriser l'émulation entre collaborateurs.

**Objectifs du POC :**
1. Tester la faisabilité technique de la solution.
2. Comprendre quelles données collecter pour évaluer l'activité physique des salariés.
3. Calculer l'impact financier des avantages proposés.

Les paramètres (taux, seuils, nombre de jours) sont stockés en base dans la table `config` et peuvent être modifiés sans toucher au code.

### Données source fournies

| Fichier | Emplacement | Contenu |
|---------|-------------|---------|
| `Donne_es_RH.xlsx` | `data/raw/` · `documentation/` | 161 salariés : identifiant, BU, adresse domicile, mode de déplacement déclaré, salaire brut |
| `Donne_es_Sportive.xlsx` | `data/raw/` · `documentation/` | 999 lignes (95 salariés avec sport déclaré) : type de sport pratiqué |

Ces deux fichiers XLSX constituent les seules données réelles du projet. Ils sont gitignorés depuis `data/raw/` (données RH sensibles) mais conservés dans `documentation/` à titre de référence. Les données d'activité sportive des 12 derniers mois sont simulées par le générateur Monte Carlo (Round 1) en attendant un branchement direct sur l'API Strava.

### Démarche et choix techniques pour le POC

Le POC couvre l'ensemble du périmètre défini dans la note de cadrage (infrastructure, simulation Strava, ETL Medallion, contrôles qualité, orchestration, Power BI), tout en faisant des **choix délibérément plus simples** que ceux qui seront retenus en production :

| Composant | Choix POC | Choix production envisagé |
|-----------|-----------|--------------------------|
| Données activités sportives | Simulateur Monte Carlo (`generate_strava.py`) | Connexion directe API Strava |
| Stockage Delta Lake | Local (`data/delta/`) via `delta-rs`, sans Spark | S3 / Azure Blob + Spark ou Databricks |
| Qualité des données | SQL maison (`quality_check.py`, 9 règles) | Great Expectations ou SODA (voir §11) |
| Orchestration | Kestra standalone (H2 embarqué, Docker local) | Kestra ou Airflow managé (cloud) |
| Pont Docker ↔ Python | Flask HTTP (voir §7.3) | Scripts containerisés dans Docker |

> L'architecture cible envisagée dans la note de cadrage — dont une vue schématique est disponible dans [documentation/Note+de+cadrage+_+POC+Avantages+Sportif_page2_image.png](documentation/Note+de+cadrage+_+POC+Avantages+Sportif_page2_image.png) — inclut certaines solutions (connexion Strava live, data lake cloud, moteur Spark) qui peuvent s'avérer **overkill** pour la phase POC. Le parti pris a été de livrer un pipeline fonctionnel de bout en bout, entièrement local, sans dépendances cloud ni licences tierces.

La migration vers l'architecture cible ne devrait pas poser de difficultés majeures : la couche Delta Lake est conçue pour être transparente au changement de stockage (un seul paramètre `local → s3://` dans `src/config.py`), et les scripts Python sont indépendants du scheduler.

---

## 2. Architecture technique du POC

> **Architecture volontairement restreinte pour la phase POC.** L'ensemble de la stack tourne localement (Docker Desktop + conda), sans service cloud ni composant externe payant. Les solutions définitives (API Strava live, data lake cloud, bases managées) ont été mises de côté pour rester maniables ; la structure Medallion Bronze / Silver / Gold garantit que la migration ne nécessitera pas de réécriture majeure des traitements.

```
Donne_es_RH.xlsx          ┐
Donne_es_Sportive.xlsx     ├──► raw_rh / raw_sport (PostgreSQL — Bronze brut)
strava_activities (simulé) ┘        │
         │                          ▼ data/delta/bronze/strava/
         ▼
   [ETL Silver — src/etl_silver.py]
   normalisation IDs, dates, modes, sports
   jointure RH ↔ Sport
   distances Google Maps API (uniquement marche/vélo)
   calcul eligible_prime
         │
         ▼
   employees (PostgreSQL — Silver)
   data/delta/silver/employees/
         │
         ▼
   [Quality Check — src/quality_check.py]
   9 règles SQL (BLOQUANT + WARNING)
   rapport HTML + data_quality_results
         │
         ▼
   [ETL Gold — src/etl_gold.py]
   prime = salaire × taux_prime  (si eligible_prime)
   journées BE = (nb_activites ≥ 15)
         │
         ▼
   avantages_calcules (PostgreSQL — Gold)
   data/delta/gold/avantages/
         │
         ▼
   [Flask Entry — src/flask_entry.py]          ◄── Round 4
   Interface web saisie manuelle
   API REST exposée à Kestra
         │                 │
         ▼                 ▼
   Slack Webhook       Kestra (Docker)
   notifications       orchestration des 6 flows
   temps réel          schedules automatiques
         │
         ▼
   [Export Power BI — src/export_power_bi.py] ◄── Round 5
   7 datasets CSV ou Excel
   5 pages rapport · DAX What-If · recalcul scénario DRH
```

### Stack complète

| Composant | Rôle | Depuis |
|-----------|------|--------|
| **PostgreSQL 16** | Base de données principale (Docker) | R1 |
| **pgAdmin 4** | Interface web PostgreSQL (Docker) | R1 |
| **pandas / openpyxl** | Lecture XLSX et manipulation DataFrames | R1 |
| **psycopg2 + SQLAlchemy** | Connexion et ORM Python → PostgreSQL | R1 |
| **delta-rs (`deltalake`)** | Écriture Delta Lake sans Spark ni JVM | R1 |
| **DuckDB** | Lecture analytique des fichiers Delta Lake locaux | R1 |
| **googlemaps** | SDK Python Google Maps API | R2 |
| **python-dotenv** | Chargement `.env` → variables d'environnement | R1 |
| **Flask** | Pont HTTP entre Kestra (Docker) et les scripts Python (hôte) | R4 |
| **Kestra** | Orchestrateur de workflows — 6 flows, schedules, UI | R4 |
| **requests** | Appels HTTP sortants (Slack webhook, Kestra API) | R4 |
| **Slack Incoming Webhooks** | Notifications temps réel dans un channel | R4 |
| **openpyxl** | Export Excel multi-onglets pour Power BI | R5 |
| **Power BI Desktop** | Rapport 5 pages, DAX What-If, connexion PostgreSQL/CSV | R5 |

### Pourquoi PostgreSQL plutôt qu'une autre base ?

Les données RH sont intrinsèquement relationnelles (salariés, activités, éligibilité, paramètres versionnés) et les 9 règles de contrôle qualité s'écrivent naturellement en SQL. PostgreSQL s'est imposé face aux alternatives :

| Critère | PostgreSQL | Alternatives écartées |
|---------|------------|----------------------|
| **Accès concurrent** | Multi-client natif — Flask + Kestra + pgAdmin écrivent simultanément | SQLite : un seul écrivain, verrou en écriture |
| **Python** | psycopg2 + SQLAlchemy matures, zéro friction avec pandas | — |
| **Power BI** | Driver ODBC officiel (psqlODBC) | — |
| **Docker** | Image officielle, comportement production-identique | — |
| **SQL analytique** | Fenêtrage, CTE, agrégations avancées utilisées dans les ETL et les contrôles qualité | MySQL : moins riche analytiquement |
| **Données structurées** | Schéma strict, jointures, clés étrangères — adapté aux données RH normalisées | MongoDB : document store inadapté ici |
| **Serveur persistant** | Connexions longue durée, écritures répétées par plusieurs process | DuckDB : excellent en lecture analytique, pas conçu pour l'accès concurrent multi-process |
| **Coût / complexité** | Docker local, gratuit, zéro dépendance externe | BigQuery, Snowflake, RDS : overkill pour un POC local |

En production, PostgreSQL peut rester en place (instance managée RDS ou Cloud SQL) ou être remplacé par un entrepôt cloud — la couche Delta Lake isole cette décision du reste du pipeline.

---

## 3. Environnement et démarrage

### 3.1 Environnement Python

```powershell
# Activer l'environnement conda du projet
conda activate datascience2

# Aller dans le dossier
cd C:\Users\karap\OpenClassRooms\projet12
```

> L'environnement `datascience2` contient tous les packages nécessaires aux quatre rounds
> (psycopg2, pandas, deltalake, duckdb, googlemaps, flask, requests, pytest…).

### 3.2 Variables d'environnement

Le fichier `.env` à la racine contient les secrets locaux (non versionné) :

```bash
# PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=poc_sport
DB_USER=admin
DB_PASSWORD=admin123

# pgAdmin
PGADMIN_EMAIL=admin@poc.local
PGADMIN_PASSWORD=admin123

# Paramètres métier (peuvent aussi être surchargés dans la table config)
TAUX_PRIME=0.05
SEUIL_MARCHE_KM=15.0
SEUIL_VELO_KM=25.0
MIN_ACTIVITES_BE=15
NB_JOURS_BE=5

# Google Maps (laisser GMAPS_MOCK_MODE=true pour éviter les frais API)
GOOGLE_MAPS_API_KEY=...
GMAPS_MOCK_MODE=false

# Slack (Round 4) — mode DRY RUN si absent
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

> `.env.example` est versionné comme template. `.env` ne l'est jamais (données sensibles).

### 3.3 Démarrer Docker

```powershell
# Démarrer PostgreSQL + pgAdmin (Round 1)
docker compose -f docker/docker-compose.postgres.yml up -d

# Démarrer Kestra (Round 4)
docker compose -f docker/docker-compose.kestra.yml up -d

# Vérifier les trois containers
docker ps
# poc_postgres   Up   0.0.0.0:5432->5432/tcp
# poc_pgadmin    Up   0.0.0.0:5050->80/tcp
# kestra         Up   0.0.0.0:8080->8080/tcp
```

> **Note Kestra** : le `docker-compose.kestra.yml` utilise `server local` (base H2 embarquée,
> parfait pour le POC) et rejoint `poc_network` pour partager le réseau avec PostgreSQL.

### 3.4 Se connecter à pgAdmin

Ouvrir **http://localhost:5050**

| Champ | Valeur |
|-------|--------|
| Email | `admin@poc.local` |
| Password | `admin123` |

Ajouter un serveur : clic droit **Servers → Register → Server**

| Onglet | Champ | Valeur |
|--------|-------|--------|
| General | Name | `poc_sport` |
| Connection | Host | `poc_postgres` ← nom du container, pas `localhost` |
| Connection | Port | `5432` |
| Connection | Database | `poc_sport` |
| Connection | Username | `admin` |
| Connection | Password | `admin123` |

> pgAdmin tourne lui-même dans Docker : il joint PostgreSQL via le réseau interne `poc_network`,
> d'où le hostname `poc_postgres` et non `localhost`.

---

## 4. Round 1 — Infra + Simulation Strava-like

### 4.1 Objectif

Mettre en place l'infrastructure complète et peupler `strava_activities` avec des données
d'activités sportives simulées pour les 161 salariés RH, afin de préparer le calcul des
journées bien-être en Round 3.

### 4.2 Logique de simulation

Le générateur (`src/generate_strava.py`) simule une année complète **2025-01-01 → 2025-12-31** :

1. **Population source** : les 95 salariés ayant déclaré un sport dans le fichier XLSX — tous simulés, sans sous-sélection.
2. **Règle par salarié et par mois** : tirage uniforme d'un nombre d'activités dans **[0, 5]**, indépendamment pour chacun des 12 mois.
3. **Pour chaque activité** : jour aléatoire dans le mois, heure réaliste (6h–20h), durée et distance tirées selon le sport.
4. **Commentaires** : 20 % des activités reçoivent un commentaire (taux fixe).
5. **Insertion** : `TRUNCATE TABLE strava_activities` puis insertion de toutes les activités générées.
6. **Reproductibilité** : option `--seed` pour figer le tirage et obtenir le même résultat à chaque run.

Les paramètres par sport (distances min/max, durées min/max) sont définis dans `src/config.py` —
`distance_m` est `NULL` pour les sports sans déplacement (escalade, tennis, football…).

### 4.3 Exécution

```powershell
# Pipeline complet Round 1
python scripts/run_round1.py

# Options
python scripts/run_round1.py --dry-run        # simule sans insérer
python scripts/run_round1.py --seed 244542299 # résultat reproductible (seed fixe)
python scripts/run_round1.py --skip-bronze    # génère sans écrire le Delta Lake
python src/bronze_writer.py                   # écriture Bronze seule
```

### 4.4 Résultats observés (seed 244542299)

| Indicateur | Valeur |
|-----------|--------|
| Salariés avec sport déclaré | 95 / 161 |
| Salariés simulés (tous actifs) | 95 / 95 |
| Activités totales générées | **2 905** |
| Moyenne par salarié | 30.6 activités/an |
| Salariés avec ≥ 15 activités/an | **95 / 95** (tous éligibles BE potentiels) |
| Sports distincts | 15 |
| Période couverte | 2025-01-01 → 2025-12-31 |
| Fichiers Parquet Bronze | 8 |
| Taille Bronze | 0.86 MB |

Répartition par sport :

| Sport | Activités | Durée moy. | Distance moy. |
|-------|-----------|------------|---------------|
| Randonnée | 527 | 149 min | 12.3 km |
| Runing *(typo conservée)* | 527 | 52 min | 10.6 km |
| Tennis | 346 | 89 min | N/A |
| Natation | 240 | 41 min | 1.8 km |
| Rugby | 196 | 76 min | N/A |
| Football | 171 | 75 min | N/A |
| Badminton | 153 | 67 min | N/A |
| Voile | 139 | 205 min | N/A |
| Judo | 106 | 75 min | N/A |
| Boxe | 100 | 60 min | N/A |
| Triathlon | 95 | 125 min | 24.1 km |
| Escalade | 89 | 135 min | N/A |
| Équitation | 80 | 91 min | N/A |
| Basketball | 75 | 75 min | N/A |
| Tennis de table | 61 | 45 min | N/A |

> `distance_m = NULL` pour les sports sans déplacement : c'est normal et attendu.
> La typo `Runing` est conservée en Bronze — elle sera corrigée en `Running` à l'ETL Silver (Round 2).

### 4.5 Vérification Bronze (DuckDB)

```python
import duckdb
conn = duckdb.connect()
conn.execute("INSTALL delta; LOAD delta;")
df = conn.execute("""
    SELECT COUNT(*) as nb_lignes, COUNT(DISTINCT employee_id) as nb_athletes,
           COUNT(DISTINCT sport_type) as nb_sports,
           MIN(date_debut)::date as premiere, MAX(date_debut)::date as derniere
    FROM delta_scan('data/delta/bronze/strava')
""").fetchdf()
print(df)
conn.close()
```

### 4.6 Vérification SQL (pgAdmin)

```sql
-- Activités par sport
SELECT sport_type, COUNT(*) as nb, ROUND(AVG(duree_s/60.0), 1) as dur_moy_min
FROM strava_activities GROUP BY sport_type ORDER BY nb DESC;

-- Logs pipeline
SELECT flow_name, statut, nb_lignes, duree_ms FROM pipeline_runs;
```

### 4.7 Tests Round 1

```powershell
pytest tests/test_round1.py -v
# Résultat attendu : 11/11 PASSED
```

- `TestPostgreSQL` (4 tests) : connexion, 9 tables présentes, `config` peuplée, `taux_prime = 0.05`
- `TestGenerator` (5 tests) : génération Running (distance + durée cohérentes), Escalade (distance NULL), typo Runing acceptée, plage 0-60 activités/an respectée, toutes les dates en 2025
- `TestInsertion` (2 tests) : dry-run retourne 0, insertion réelle + vérification + nettoyage
- `TestBronze` (3 tests) : dossier Bronze existe + `_delta_log` présent, lisible par DuckDB, schéma correct

---

## 5. Round 2 — Ingestion XLSX + ETL Silver + Google Maps

### 5.1 Objectif

Charger les données RH et sportives réelles, les normaliser, calculer les distances
domicile ↔ bureau, et déterminer l'éligibilité à la prime pour chaque salarié.

### 5.2 Pipeline complet

```
Donne_es_RH.xlsx (161 lignes)
        ↓  src/ingest_xlsx.py
raw_rh (PostgreSQL, données brutes non transformées)

Donne_es_Sportive.xlsx (999 lignes, 95 avec sport)
        ↓  src/ingest_xlsx.py
raw_sport (PostgreSQL, données brutes)

raw_rh + raw_sport
        ↓  src/etl_silver.py — étape 1 : normalisation
        │   IDs : '59019.0' → '59019'
        │   Dates : serial Excel ou ISO → date Python
        │   Salaires : '30940.0' → 30940.00
        │   Modes : voir tableau ci-dessous
        │   Sports : 'Runing' → 'Running' (typo corrigée)
        ↓  étape 2 : jointure RH ↔ Sport (LEFT JOIN sur id_salarie)
        ↓  étape 3 : distances Google Maps (UNIQUEMENT modes éligibles)
        ↓  étape 4 : calcul eligible_prime
        ↓
employees (PostgreSQL — table Silver)
data/delta/silver/employees/ (Delta Lake Silver)
```

### 5.3 Normalisation des modes de déplacement

| Valeur brute dans le XLSX | Valeur normalisée | Éligible prime |
|--------------------------|-------------------|---------------|
| `Marche/running` | `marche_running` | Oui (si distance ≤ 15 km) |
| `Vélo/Trottinette/Autres` | `velo_trottinette` | Oui (si distance ≤ 25 km) |
| `Transports en commun` | `transports_commun` | Non |
| `véhicule thermique/électrique` | `vehicule` | Non |

### 5.4 Règle d'éligibilité à la prime

```
eligible_prime = True
    si  mode IN ('marche_running', 'velo_trottinette')
    ET  distance_bureau_km ≤ seuil_correspondant
```

> **Optimisation importante** : les salariés en `transports_commun` ou `vehicule` ne peuvent
> jamais être éligibles. Leur distance n'est donc jamais calculée, ce qui évite ~40 % d'appels
> API inutiles et garantit `distance_bureau_km = NULL` pour ces modes en base.

### 5.5 Calcul des distances — Google Maps API

Pour chaque salarié éligible (`marche_running` ou `velo_trottinette`), le client
(`src/gmaps_client.py`) calcule la distance domicile ↔ bureau en trois étapes :

```
1. Cache PostgreSQL (table gmaps_cache)
   → hash SHA-256(adresse_origine + adresse_bureau + mode_gmaps)
   → si le hash existe en base : retourne la distance immédiatement (0 appel API)

2. Si absent du cache :
   Mode réel  → appel Google Maps Distance Matrix API
   Mode mock  → distance pseudo-réaliste déterministe (hash de l'adresse comme seed)

3. Mise en cache du résultat pour les runs suivants
```

Le mode mock est activé par `GMAPS_MOCK_MODE=true` ou en l'absence de clé API : distances
déterministes (même adresse → même distance) basées sur des distributions réalistes autour
de Montpellier/Lattes.

### 5.6 Exécution

```powershell
# Pipeline complet Round 2
python scripts/run_round2.py

# Options
python scripts/run_round2.py --dry-run       # affiche sans écrire en base
python scripts/run_round2.py --skip-gmaps    # ETL sans distances (test rapide)
python scripts/run_round2.py --skip-ingest   # ETL seul si raw_rh déjà peuplé
```

### 5.7 Résultats attendus après Round 2

| Indicateur | Valeur |
|-----------|--------|
| `raw_rh` | 161 lignes |
| `raw_sport` | 999 lignes |
| `employees` | 161 lignes normalisées |
| Éligibles prime | 68 / 161 (~42 %) |
| Distance moyenne (marche/vélo) | 7.1 km |
| `gmaps_cache` | ~67 entrées (walking + bicycling uniquement) |
| Delta Lake Silver | `data/delta/silver/employees/` |

### 5.8 Tests Round 2

```powershell
pytest tests/test_round2.py -v
# Résultat attendu : 49/49 PASSED
```

- `TestNormalisation` (14 tests) : `normalize_id`, `normalize_date`, `normalize_salaire`, `normalize_mode_deplacement`, `normalize_sport`
- `TestEligibilite` (11 tests) : marche/vélo sous et sur seuil, TC et véhicule jamais éligibles
- `TestGmapsMock` (7 tests) : distance non nulle, déterministe, géocodage, filtrage modes non éligibles
- `TestIngestDryRun` (3 tests) : dry-run sans insertion, structure XLSX RH et Sport
- `TestSilverDB` (7 tests) : table peuplée, éligibles cohérents, typo Runing absente, cache uniquement walking/bicycling
- `TestSilverDelta` (2 tests) : dossier Delta existe, lisible par DuckDB

---

## 6. Round 3 — Quality Check + ETL Gold

### 6.1 Objectif

Round 3 produit la **couche Gold** du pipeline : validation de la qualité des données
(9 règles SQL) puis calcul effectif des avantages financiers.

### 6.2 Exécution

```powershell
# Pipeline complet Round 3
python scripts/run_round3.py

# Options
python scripts/run_round3.py --dry-run          # calcule sans écrire en base
python scripts/run_round3.py --skip-qc          # sauter les contrôles qualité
python scripts/run_round3.py --fail-fast        # stoppe si une règle BLOQUANTE échoue
python scripts/run_round3.py --params-version v2.0_taux7pct
```

### 6.3 Quality Check — les 9 règles

| # | Règle | Table | Sévérité | Ce qu'elle vérifie |
|---|-------|-------|----------|--------------------|
| ① | `anomalie_declaration_marche` | `employees` | WARNING | Marche déclarée mais distance > 15 km |
| ② | `anomalie_declaration_velo` | `employees` | WARNING | Vélo déclaré mais distance > 25 km |
| ③ | `eligible_prime_coherence_etl` | `employees` | **BLOQUANT** | Aucun `eligible_prime=True` ne viole les règles métier |
| ④ | `mode_deplacement_enum` | `employees` | **BLOQUANT** | Modes dans les 4 valeurs de l'enum normalisé |
| ⑤ | `id_sans_decimal` | `employees` | **BLOQUANT** | Aucun ID contenant `.` |
| ⑥ | `salaire_plausible` | `employees` | WARNING | `salaire_brut` dans [1, 500 000 €] |
| ⑦ | `distance_sport_positive` | `strava_activities` | **BLOQUANT** | `distance_m > 0` si non NULL |
| ⑧ | `employee_id_fk` | `strava_activities` | **BLOQUANT** | Tous les `employee_id` existent dans `employees` |
| ⑨ | `dates_strava_fenetre_2025` | `strava_activities` | WARNING | Dates dans la fenêtre 2025 |

### 6.4 ETL Gold — résultats

| Indicateur | Valeur |
|-----------|--------|
| `avantages_calcules` | 161 lignes |
| Éligibles prime | 68 / 161 (42 %) |
| dont marche_running ≤ 15 km | 14 |
| dont velo_trottinette ≤ 25 km | 54 |
| Coût total primes | ~106 427 € |
| Éligibles journées BE (≥ 15 activités/an) | ~47 / 161 |
| Total journées bien-être accordées | ~235 jours |
| Delta Lake Gold | `data/delta/gold/avantages/` |
| Rapport HTML qualité | `reports/quality_report.html` |

### 6.5 Paramètres modifiables sans code

```sql
-- Simuler un taux à 7 %
UPDATE config SET valeur = '0.07' WHERE cle = 'taux_prime';
UPDATE config SET valeur = 'v2.0_taux7pct' WHERE cle = 'params_version';
-- Revenir au taux standard
UPDATE config SET valeur = '0.05' WHERE cle = 'taux_prime';
UPDATE config SET valeur = 'v1.0' WHERE cle = 'params_version';
```

> La stratégie **TRUNCATE + INSERT** de l'ETL Gold garantit qu'un changement de paramètre
> recalcule tout de façon cohérente. Deux runs avec les mêmes paramètres sont identiques.

### 6.6 Tests Round 3

```powershell
pytest tests/test_round3.py -v
# Résultat attendu : 32 passed, 1 skipped
```

- `TestEtlGold` (7 tests) : paramètres config, formule prime, dry-run, journées BE
- `TestQualityCheck` (13 tests) : chaque règle SQL, persistance, rapport HTML
- `TestAvantagesDB` (6 tests + 1 skipped) : 161 lignes, éligibles = 68, primes cohérentes
- `TestGoldDelta` (4 tests) : Delta Lake Gold lisible, schéma correct

---

## 7. Round 4 — Orchestration Kestra + Flask + Notifications Slack

### 7.1 Objectif

Automatiser le pipeline via **Kestra** et déclencher des **notifications Slack** en temps
réel à chaque saisie manuelle d'activité sportive via une interface web **Flask**.

Le livrable principal : une saisie dans l'interface Flask → message Slack personnalisé
dans le channel en moins de 5 secondes.

### 7.2 Nouveaux fichiers

```
projet12/
├── kestra/
│   └── flows/
│       ├── 00_pipeline_complet.yml  ← Orchestre les 5 flows dans l'ordre
│       ├── 01_ingest_xlsx.yml       ← Déclenché manuellement (nouveau fichier XLSX)
│       ├── 02_generate_strava.yml   ← Re-simulation Monte Carlo
│       ├── 03_etl_silver_gold.yml   ← Schedule quotidien 6h (ETL complet)
│       ├── 04_quality_check.yml     ← Après ETL ou incident
│       └── 05_notify_slack.yml      ← Polling */5 min (activités manuelles)
│
├── src/
│   ├── flask_entry.py               ← Interface web + API REST
│   └── slack_notifier.py            ← Webhook Slack + formatage Block Kit
│
├── scripts/
│   └── run_round4.py                ← Vérification setup complet
│
├── tests/
│   └── test_round4.py               ← 31 tests (formatter, Flask, flows YAML)
│
└── docker/
    └── docker-compose.kestra.yml    ← Kestra standalone (server local)
```

### 7.3 Choix technique clé : Flask comme pont HTTP

**Problème** : Kestra tourne dans un container Docker. Les scripts Python (`etl_silver.py`,
`slack_notifier.py`…) tournent sur l'hôte Windows dans l'environnement conda `datascience2`.
Docker ne peut pas exécuter directement des scripts Python de l'hôte sans montage de volume
complexe ni installation de Python dans le container.

**Solution retenue** : Flask expose les scripts comme des endpoints HTTP. Kestra appelle
`http://host.docker.internal:5001/api/etl` — Flask reçoit la requête et exécute le script
Python correspondant sur l'hôte. Ce découplage est total : Kestra ne connaît pas conda,
les scripts Python ne connaissent pas Kestra.

```
Kestra (Docker)
    POST http://host.docker.internal:5001/api/etl
         │
         ▼
    Flask (hôte Windows, port 5001)
         │  lance etl_silver.py + etl_gold.py
         ▼
    PostgreSQL ← résultats écrits
```

**Alternative écartée** : monter le dossier projet dans le container Kestra et y installer
Python/conda. Trop fragile sur Windows (chemins, permissions) et couple fort Kestra ↔
environnement Python.

### 7.4 Architecture complète Round 4

```
Browser
  └──► GET / → Flask UI (localhost:5001) → formulaire saisie Strava
         └──► POST /api/activity → INSERT strava_activities (source='manual')
                └──► slack_notifier.notify_by_activity_id() → Slack 💬

Kestra (Docker, localhost:8080)
  └──► GET  http://host.docker.internal:5001/health      → health check
  └──► POST http://host.docker.internal:5001/api/ingest  → ingest_xlsx.py
  └──► POST http://host.docker.internal:5001/api/generate→ generate_strava.py
  └──► POST http://host.docker.internal:5001/api/etl     → etl_silver.py + etl_gold.py
  └──► POST http://host.docker.internal:5001/api/quality → quality_check.py
  └──► POST http://host.docker.internal:5001/api/notify  → slack_notifier.py (poll)
```

### 7.5 Les 6 flows Kestra

| # | Flow ID | Namespace | Schedule | Déclencheur |
|---|---------|-----------|----------|-------------|
| 00 | `pipeline_complet` | `poc.avantages_sportifs` | Manuel | Démo / restitution — orchestre les 5 flows dans l'ordre |
| 01 | `ingest_xlsx` | `poc.avantages_sportifs` | Manuel | Nouveau fichier XLSX déposé |
| 02 | `generate_strava` | `poc.avantages_sportifs` | Manuel | Re-simulation Monte Carlo |
| 03 | `etl_silver_gold` | `poc.avantages_sportifs` | `0 6 * * *` | Recalcul journalier automatique à 6h |
| 04 | `quality_check` | `poc.avantages_sportifs` | Manuel | Audit qualité après ETL ou incident |
| 05 | `notify_slack` | `poc.avantages_sportifs` | `*/5 * * * *` | Polling toutes les 5 minutes |

> **Flow 00 `pipeline_complet`** : utilise `io.kestra.plugin.core.flow.Subflow` pour
> enchaîner les 5 flows dans l'ordre logique. Le Quality Check est en mode `transmitFailed: false`
> (un warning QC ne bloque pas la suite du pipeline). Les erreurs critiques loggent en base et
> envoient une alerte Slack.

### 7.6 Choix technique : `source='manual'` dans strava_activities

La colonne `source` dans `strava_activities` distingue :
- `'strava'` : activités générées par le simulateur Monte Carlo (Round 1)
- `'manual'` : activités saisies via Flask (Round 4)

Le notifier Slack ne poll que les activités `source='manual'` créées dans les 5 dernières
minutes. Cela évite d'envoyer des milliers de messages pour les 2 905 activités simulées.

### 7.7 Choix technique : Slack Block Kit

Les messages Slack utilisent l'API **Block Kit** (payload JSON structuré) plutôt que du
texte brut, pour un rendu riche avec sections, champs et contexte :

```
🏃 Bravo Marie Dupont ! 12,0 km en 55 min ! Quelle énergie ! 🔥
"Tour du lac ce matin ! 🌅"

Distance    Durée     BU
12,0 km     55 min    Marketing
🏃 Running · POC Avantages Sportifs
```

Chaque sport dispose d'un emoji dédié et de 2-3 templates de message tirés aléatoirement
(même salarié + même sport = messages différents à chaque activité).

En l'absence de `SLACK_WEBHOOK_URL` dans `.env`, le notifier passe automatiquement en
**mode DRY RUN** : le message est loggé dans `logs/slack_notifier.log` sans être envoyé.

### 7.8 Démarrage Round 4

#### Prérequis : Rounds 1→3 validés

```powershell
# Vérifier que les données Gold sont présentes
python scripts/run_round3.py --dry-run
```

#### Étape 1 — Démarrer Kestra

```powershell
docker compose -f docker/docker-compose.kestra.yml up -d
# Attendre ~30 secondes puis ouvrir http://localhost:8080
```

#### Étape 2 — Démarrer Flask

```powershell
# Terminal dédié — garder ouvert pendant la démo
conda activate datascience2
python src/flask_entry.py
# Sortie : Flask démarré : http://localhost:5001
```

#### Étape 3 — Vérifier le setup complet

```powershell
python scripts/run_round4.py
```

Sortie attendue :
```
  [1/6] PostgreSQL...                     ✅
  [2/6] Données (employees + avantages)...✅  (161 employees, 161 avantages)
  [3/6] Flask Entry (port 5001)...        ✅
  [4/6] Kestra (http://localhost:8080)... ✅
  [5/6] Flows Kestra YAML...              ✅  (6 flows présents dans kestra/flows/)
  [6/6] Slack Webhook URL...              ✅  (URL configurée)
```

#### Étape 4 — Les flows sont déjà déployés dans Kestra

Les 6 flows ont été déployés automatiquement via l'API Kestra dans le namespace
`poc.avantages_sportifs`. Ils sont visibles dans **http://localhost:8080 → Flows**.

Pour redéployer manuellement si nécessaire :

```powershell
$cred = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("email@example.com:motdepasse"))
$headers = @{Authorization="Basic $cred"; "Content-Type"="application/x-yaml"}
Get-ChildItem kestra\flows\*.yml | ForEach-Object {
    $yaml = Get-Content $_.FullName -Raw -Encoding UTF8
    Invoke-WebRequest -Uri "http://localhost:8080/api/v1/main/flows" `
                      -Method POST -Body $yaml -Headers $headers -UseBasicParsing
    Write-Host "Deploye : $($_.Name)"
}
```

### 7.9 Endpoints Flask — référence complète

| Méthode | Endpoint | Body JSON | Description |
|---------|----------|-----------|-------------|
| `GET`  | `/` | — | Interface UI saisie manuelle |
| `GET`  | `/health` | — | Health check PostgreSQL + statut |
| `GET`  | `/api/stats` | — | KPI Gold (JSON) |
| `POST` | `/api/activity` | `employee_id, sport_type, duree_min, distance_km?, commentaire?` | Saisie manuelle + Slack immédiat |
| `POST` | `/api/ingest` | `dry_run?` | Lance `ingest_xlsx.py` |
| `POST` | `/api/generate` | `dry_run?` | Lance `generate_strava.py` |
| `POST` | `/api/etl` | `dry_run?, skip_gmaps?` | ETL Silver + Gold |
| `POST` | `/api/quality` | `suite?, fail_fast?` | Quality check (9 règles) |
| `POST` | `/api/notify` | `since_minutes?, dry_run?` | Poll Slack (activités manuelles) |

```bash
# Saisie manuelle (démo live)
curl -X POST http://localhost:5001/api/activity \
     -H "Content-Type: application/json" \
     -d '{"employee_id":"12345","sport_type":"Running","duree_min":46,"distance_km":10.8,"commentaire":"Belle sortie !"}'

# Test Slack sans envoi réel
curl -X POST http://localhost:5001/api/notify \
     -H "Content-Type: application/json" \
     -d '{"dry_run":true,"since_minutes":60}'
```

### 7.10 Scénario démo live (5 minutes)

```
1. Ouvrir http://localhost:5001
   → Interface de saisie manuelle (simulation app Strava production)

2. Saisir une activité
   → Salarié : [choisir] · Sport : Running · Distance : 12 km · Durée : 55 min
   → Commentaire : "Tour du lac ce matin !"
   → Clic sur Enregistrer

3. Observer Slack en temps réel
   → Message dans le channel en < 5 secondes :
   "🏃 Bravo Marie Dupont ! 12,0 km en 55 min ! Quelle énergie ! 🔥
   'Tour du lac ce matin !'"

4. Montrer Kestra
   → http://localhost:8080 → Flows → notify_slack → Dernier run
   → Kestra orchestre le polling automatique toutes les 5 minutes

5. Montrer les KPI
   → http://localhost:5001/api/stats
   → Impact des nouvelles données en temps réel
```

### 7.11 Tests Round 4

```powershell
pytest tests/test_round4.py -v
# Résultat attendu : 31/31 PASSED
```

- `TestSlackFormatter` (14 tests) : formatage distances/durées, templates par sport, payload Block Kit, dry-run
- `TestFlaskHealth` (4 tests) : endpoint `/health`, `/`, `/api/stats`
- `TestFlaskActivityAPI` (3 tests) : validation champs manquants, durée invalide, insertion + notification
- `TestFlaskPipelineAPI` (2 tests) : ingest dry-run, notify dry-run
- `TestKestraFlows` (8 tests) : répertoire flows présent, 5 fichiers YAML valides, schedules présents dans 03 et 05

### 7.12 Corrections de compatibilité Kestra 1.3.x

Deux incompatibilités détectées et corrigées lors du déploiement automatique :

| Fichier | Problème | Correction |
|---------|----------|------------|
| `05_notify_slack.yml` | `type: INTEGER` invalide | → `type: INT` |
| `00_pipeline_complet.yml` | `io.kestra.plugin.flow.Subflow` déprécié | → `io.kestra.plugin.core.flow.Subflow` |

---

## 8. Round 5 — Power BI + Export CSV + Clôture projet

### 8.1 Objectif

Produire une **restitution visuelle complète** à partir des données Gold : 7 datasets exportés
pour Power BI, un rapport 5 pages opérationnel avec DAX What-If, et la livraison des
corrections R4 manquantes (`docker-compose.kestra.yml`, flow `00_pipeline_complet.yml`
corrigé Kestra 1.3.x).

### 8.2 Nouveaux fichiers

```
projet12/
├── docker/
│   └── docker-compose.kestra.yml        ← livré R5 (manquait en R4)
│
├── kestra/flows/
│   └── 00_pipeline_complet.yml          ← corrigé Kestra 1.3.x
│
├── src/
│   └── export_power_bi.py               ← export 7 datasets CSV ou Excel
│
├── sql/
│   └── power_bi_queries.sql             ← 5 requêtes SQL (une par page PBI)
│
├── reports/
│   ├── dax_measures.md                  ← mesures DAX copiables
│   └── power_bi/                        ← créé automatiquement par export
│       ├── vue_globale.csv
│       ├── primes_sportives.csv
│       ├── journees_bienetre.csv
│       ├── activites_sportives.csv
│       ├── anomalies_qualite.csv
│       ├── config_params.csv
│       └── pipeline_logs.csv
│
├── scripts/
│   └── run_round5.py
│
└── tests/
    └── test_round5.py
```

### 8.3 Connexion Power BI — 3 options

| Option | Description | Prérequis |
|--------|-------------|-----------|
| **A — PostgreSQL ODBC** | Connexion native, refresh en 1 clic | Driver psqlODBC |
| **B — CSV** | Fichiers plats dans `reports/power_bi/` | Aucun |
| **C — Excel** | Classeur multi-onglets | Aucun |

```powershell
# Option B/C : générer les exports
python src/export_power_bi.py              # CSV (défaut)
python src/export_power_bi.py --format excel  # Excel
```

### 8.4 Les 5 pages du rapport Power BI

| Page | Source CSV | Visuels clés |
|------|------------|-------------|
| 1 — Vue Globale | `vue_globale.csv` | KPI coût total, % éligibles, répartition par BU |
| 2 — Primes Sportives | `primes_sportives.csv` | Scatter salaire/prime, slicer BU, What-If taux |
| 3 — Journées Bien-être | `journees_bienetre.csv` | Distribution activités, ligne seuil 15, treemap sport |
| 4 — Activités Sportives | `activites_sportives.csv` | Courbe mensuelle, top sports, heatmap BU × mois |
| 5 — Anomalies & Qualité | `anomalies_qualite.csv` | Tableau règles QC, historique runs, logs pipeline |

**Paramètre What-If (page 2) :** glissière de taux (1 % → 15 %) + mesure DAX `Coût Simulé Primes`
pour simuler l'impact financier sans toucher aux données.

### 8.5 Recalcul avec un nouveau taux (scénario DRH)

```sql
-- Passer à 7 %
UPDATE config SET valeur = '0.07' WHERE cle = 'taux_prime';
UPDATE config SET valeur = 'v2.0_taux7pct' WHERE cle = 'params_version';
```

```powershell
python scripts/run_round3.py --params-version v2.0_taux7pct
python src/export_power_bi.py
# → Actualiser Power BI Desktop (Ctrl+Alt+F5)
```

### 8.6 Exécution

```powershell
# Vérification complète R1→R5 + export CSV
python scripts/run_round5.py

# Export seul
python src/export_power_bi.py
```

Sortie attendue :
```
  ✅ R1 strava_activities  total_acts=2905 | athletes=95 | sports=15
  ✅ R2 employees          total_emp=161  | nb_prime=68 | avec_dist=67
  ✅ R3 avantages_calcules total_gold=161 | eligibles=68 | cout_primes=106427.50
  ✅ R3 data_quality       total_qc=9    | qc_ok=9
  ✅ R4 strava manual      manual_acts=X
  📊 Export CSV → reports/power_bi/ (7 fichiers)
```

### 8.7 KPI finaux — tableau de bord complet

| Indicateur | Valeur |
|-----------|--------|
| Salariés total | 161 |
| Éligibles prime (marche + vélo) | **68** (42 %) |
| → dont marche/running ≤ 15 km | 14 |
| → dont vélo/trottinette ≤ 25 km | 54 |
| Coût total primes (taux 5 %) | **~106 427 €** |
| Prime moyenne | **~1 565 €** |
| Éligibles journées BE (≥ 15 activités) | **~47** (29 %) |
| Total jours bien-être accordés | **~235 jours** |
| Activités simulées (seed 244542299) | **2 905** |
| Règles qualité passées | **9 / 9** |
| Tests automatisés | **~158 passed** |

### 8.8 Tests Round 5

```powershell
pytest tests/test_round5.py -v
# Résultat attendu : ~35 PASSED
```

- `TestExportCSV` (10 tests) : 7 fichiers CSV créés, 161 lignes primes, 68 éligibles, 9 règles QC, encodage UTF-8 BOM
- `TestExportExcel` (3 tests) : fichier créé, ≥ 5 onglets, feuille primes correcte
- `TestEndToEndCohérence` (13 tests) : cohérence R1→R4 (activités, employees, avantages, distances, gmaps_cache)
- `TestDeltaLakeTroisCouches` (4 tests) : Bronze/Silver/Gold lisibles par DuckDB
- `TestFichiersInfrastructure` (6 tests) : `docker-compose.kestra.yml` livré, flow 00 corrigé Kestra 1.3.x, SQL PBI présent

---

## 9. Schéma de base de données

9 tables créées automatiquement par [sql/init.sql](sql/init.sql) au premier démarrage Docker.

| Table | Rôle |
|-------|------|
| `raw_rh` | Données RH brutes telles que lues depuis le XLSX |
| `raw_sport` | Déclarations sportives brutes |
| `strava_activities` | Activités sportives — colonne `source` : `'strava'` (simulé) ou `'manual'` (Flask R4) |
| `employees` | Données Silver : RH normalisées + distances + éligibilité |
| `avantages_calcules` | Gold : primes et journées bien-être calculées |
| `config` | Paramètres métier versionnés (`taux_prime`, seuils, etc.) |
| `gmaps_cache` | Cache des appels Google Maps (évite les appels répétés) |
| `pipeline_runs` | Journal d'exécution de tous les scripts |
| `data_quality_results` | Résultats des contrôles qualité (Round 3) |

Paramètres par défaut dans `config` :

| Clé | Valeur | Description |
|-----|--------|-------------|
| `taux_prime` | `0.05` | Prime sportive = 5 % du salaire brut |
| `seuil_marche_km` | `15.0` | Distance max pour marche/running |
| `seuil_velo_km` | `25.0` | Distance max pour vélo/trottinette |
| `min_activites_be` | `15` | Activités min pour les journées bien-être |
| `nb_jours_bienetre` | `5` | Nombre de journées bien-être accordées |
| `adresse_bureau` | `1362 Av. des Platanes, 34970 Lattes` | Adresse de référence pour les distances |

---

## 10. Architecture Delta Lake

Delta Lake est une **couche de stockage ACID** posée au-dessus de fichiers Parquet.
Chaque écriture produit un fichier JSON numéroté dans `_delta_log/` qui décrit l'opération.

```
data/delta/
├── bronze/
│   └── strava/                        ← Round 1 (strava_activities brutes)
│       ├── part-0001-xxx.parquet
│       └── _delta_log/
├── silver/
│   └── employees/                     ← Round 2 (employees normalisés + enrichis)
│       ├── part-0001-xxx.parquet
│       └── _delta_log/
└── gold/
    └── avantages/                     ← Round 3 (primes + journées BE calculées)
        ├── part-0001-xxx.parquet
        └── _delta_log/
```

| Couche | Source | Transformation |
|--------|--------|----------------|
| **Bronze** | PostgreSQL `strava_activities` | Aucune — données brutes telles quelles |
| **Silver** | `raw_rh` + `raw_sport` + Google Maps | Normalisation, jointure, distances, éligibilité |
| **Gold** | `employees` + `strava_activities` | Calcul primes (5 %) et journées bien-être (≥ 15 activités) |

**Lecture avec DuckDB :**

```python
conn.execute("INSTALL delta; LOAD delta;")
conn.execute("SELECT * FROM delta_scan('data/delta/silver/employees')")
```

**Migration vers S3 (production)** : changer uniquement la constante `BRONZE_DIR` dans
`src/config.py` — `write_deltalake` accepte indifféremment un chemin local ou `s3://`.

---

## 11. Évolution vers Great Expectations

> Cette section documente comment le module `quality_check.py` (Round 3) pourrait être
> remplacé ou complété par **Great Expectations (GX)**, le framework de qualité de données
> le plus utilisé en production. Elle sert de feuille de route si le POC passe en
> industrialisation.

### 10.1 Pourquoi Great Expectations ?

| Besoin | quality_check.py actuel | Great Expectations |
|--------|------------------------|--------------------|
| Ajouter une règle | Écrire du SQL + Python | Configurer une *Expectation* en YAML ou Python |
| Profiling automatique | ❌ Manuel | ✅ `UserConfigurableProfiler` génère les règles depuis les données |
| Rapport HTML riche | Basique (fait maison) | ✅ Data Docs — HTML interactif auto-généré |
| Historique des runs | Table SQL maison | ✅ `ValidationResultStore` natif |
| Intégration Kestra | Script Python classique | ✅ Opérateurs dédiés |
| Versionning des suites | Dans le code | ✅ `ExpectationSuite` versionnée en JSON |
| Alertes Slack | ❌ (fait manuellement en R4) | ✅ Actions post-validation natives |

### 10.2 Correspondance règles actuelles → Expectations GX

| Règle actuelle | Expectation GX équivalente |
|----------------|---------------------------|
| `id_sans_decimal` | `expect_column_values_to_not_match_regex(column="id", regex=r"\.")` |
| `mode_deplacement_enum` | `expect_column_values_to_be_in_set(column="mode_deplacement", value_set=[...])` |
| `distance_sport_positive` | `expect_column_values_to_be_between(column="distance_m", min_value=1)` |
| `employee_id_fk` | Custom SQL Expectation |
| `anomalie_declaration_marche` | `expect_column_values_to_be_between(max_value=15)` avec filtre |
| `dates_strava_fenetre_2025` | `expect_column_values_to_be_between(min="2025-01-01", max="2025-12-31")` |

> **Recommandation** : conserver `quality_check.py` pour le POC. Migrer vers GX si le
> pipeline passe en production avec plusieurs tables, plusieurs sources, ou plusieurs équipes.

---

## 12. Dépannage

| Problème | Cause | Solution |
|----------|-------|----------|
| `Connection refused` sur pgAdmin | Container PostgreSQL arrêté | `docker compose -f docker/docker-compose.postgres.yml up -d` |
| pgAdmin : host `localhost` refusé | pgAdmin est dans Docker | Utiliser `poc_postgres` comme hostname |
| `FileNotFoundError: Donne_es_RH.xlsx` | XLSX absent de `data/raw/` | Copier les fichiers dans `data/raw/` |
| `can't adapt type 'NAType'` | `pd.NA` passé à psycopg2 | Corrigé dans `etl_silver.py` (`_v()` helper) |
| `Invalid data type for Delta Lake: Null` | Colonne 100 % nulle sans type | Corrigé dans `write_silver_delta` (cast PyArrow) |
| `driving`/`transit` dans `gmaps_cache` | Run avant le fix du filtrage | `DELETE FROM gmaps_cache WHERE mode_transport IN ('driving', 'transit')` |
| `ModuleNotFoundError` | Mauvais environnement actif | `conda activate datascience2` |
| `INSTALL delta` DuckDB lent | Premier téléchargement | Normal — attendre ~30 s |
| Primes toutes à 0 | `salaire_brut IS NULL` en base | Relancer `ingest_xlsx.py` puis `etl_silver.py` puis `run_round3.py` |
| `test_idempotence` bloque PostgreSQL | `TRUNCATE` + connexion `module-scoped` pytest | Marqué `@pytest.mark.skip` — pas un bug applicatif |
| Rapport HTML absent | `reports/` n'existe pas | `mkdir reports` |
| `Flask ModuleNotFoundError: flask` | Flask non installé | `pip install flask requests pyyaml` |
| Kestra "Connection refused" sur :5001 | Flask non démarré | `python src/flask_entry.py` |
| Slack message non reçu | Webhook absent ou invalide | Vérifier `SLACK_WEBHOOK_URL` dans `.env` |
| `host.docker.internal` refusé | Docker Desktop ancien | Remplacer par l'IP hôte : `ipconfig` → `172.x.x.x:5001` |
| Flow Kestra 404 | Namespace incorrect dans le YAML | Vérifier `namespace: poc.avantages_sportifs` |
| Activité insérée mais pas de Slack | `source` ≠ `'manual'` | Les activités du générateur ne sont pas notifiées (normal) |
| Kestra `UnknownHostException: postgres` | Kestra pas sur `poc_network` | Utiliser `docker-compose.kestra.yml` (rejoint `poc_network`) |
| Flow Kestra `422 Invalid type: INTEGER` | Type invalide en Kestra 1.3.x | Utiliser `INT` (et non `INTEGER`) dans les inputs |
| Flow Kestra `422 io.kestra.plugin.flow.Subflow` | Plugin déprécié | Utiliser `io.kestra.plugin.core.flow.Subflow` |
| `FileNotFoundError: reports/power_bi/` | Dossier absent | `mkdir reports/power_bi` |
| `ModuleNotFoundError: openpyxl` | Paquet absent | `pip install openpyxl` |
| CSV vides (0 lignes) | `avantages_calcules` vide | Relancer `python scripts/run_round3.py` |
| Power BI "Impossible de se connecter" | PostgreSQL arrêté | `docker compose -f docker/docker-compose.postgres.yml up -d` |
| ODBC "Driver not found" | psqlODBC non installé | Télécharger sur postgresql.org/ftp/odbc |
| Kestra "Unknown network poc_network" | PostgreSQL non démarré en premier | Démarrer `postgres` avant `kestra` |
| Taux What-If ne change pas le coût | Mesure DAX non liée | Utiliser la mesure `Coût Simulé Primes` de `reports/dax_measures.md` |

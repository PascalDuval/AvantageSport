# Champs Calculés Tableau — POC Avantages Sportifs

> Copier-coller ces formules dans Tableau Desktop :
> **Analyse → Créer un champ calculé** (ou clic droit sur un champ → Créer → Champ calculé).
> Ils s'appuient sur les sources de données exportées par `export_tableau.py`.

---

## Source principale : `primes_sportives`

---

## Champs calculés — Primes

```tableau
// Nb Total Salariés
COUNT([Id_Salarie])

// Nb Éligibles Prime
SUM(IF [Eligible_Prime] THEN 1 ELSE 0 END)

// % Éligibles Prime
SUM(IF [Eligible_Prime] THEN 1 ELSE 0 END) / COUNT([Id_Salarie])
→ Formater en pourcentage

// Coût Total Primes
SUM([Montant_Prime_Euros])

// Prime Moyenne (éligibles seulement)
AVG(IF [Eligible_Prime] THEN [Montant_Prime_Euros] END)

// Salaire Moyen
AVG([Salaire_Brut])
```

---

## Champs calculés — Journées Bien-être

```tableau
// Nb Éligibles Jours BE
SUM(IF [Eligible_Jours_Be] THEN 1 ELSE 0 END)

// % Éligibles BE
SUM(IF [Eligible_Jours_Be] THEN 1 ELSE 0 END) / COUNT([Id_Salarie])

// Total Jours BE Accordés
SUM([Nb_Jours_Be])

// Moy Activités par Salarié
AVG([Nb_Activites_12m])

// Max Activités
MAX([Nb_Activites_12m])
```

---

## Champs calculés — Vue Globale (source `vue_globale`)

```tableau
// Coût Total Global
SUM([Cout_Prime_Euros])

// Nb Primes Total
SUM([Nb_Eligible_Prime])

// Total Jours BE
SUM([Total_Jours_Be])

// Coût par Salarié Éligible
SUM([Cout_Prime_Euros]) / SUM([Nb_Eligible_Prime])
```

---

## Paramètre Tableau — Simulation taux de prime

> **Analyse → Créer un paramètre** → Nom : "Taux Prime Simulation"

| Réglage | Valeur |
|---------|--------|
| Type de données | Flottant |
| Valeur actuelle | 0,05 |
| Plage — Min | 0,01 |
| Plage — Max | 0,20 |
| Taille du pas | 0,01 |

```tableau
// Champ calculé : Prime Simulée
[Salaire_Brut] * [Taux Prime Simulation]

// Champ calculé : Coût Simulé Total
SUM([Salaire_Brut] * [Taux Prime Simulation])
// → Filtre : Eligible_Prime = True

// Champ calculé : Écart vs Taux Actuel
SUM([Salaire_Brut] * [Taux Prime Simulation])
- SUM([Montant_Prime_Euros])
// → Filtre : Eligible_Prime = True
```

> Afficher le contrôle : clic droit sur le paramètre → **Afficher le contrôle de paramètre**
> → Un curseur apparaît sur le dashboard pour la démo live

---

## Champs calculés — Activités sportives (source `activites_sportives`)

```tableau
// Nb Total Activités
SUM([Nb_Activites])

// Nb Activités Simulées (Monte Carlo)
SUM(IF [Source] = "generator" THEN [Nb_Activites] END)

// Nb Activités Manuelles (saisies Flask)
SUM(IF [Source] = "manual" THEN [Nb_Activites] END)

// Distance Totale Km
SUM([Distance_Tot_Km])

// Distance Moyenne Km
AVG([Distance_Moy_Km])

// Trimestre
"T" + STR(INT(([Num_Mois] - 1) / 3) + 1) + " " + STR([Annee])
```

---

## Champs calculés — Qualité SODA (source `anomalies_qualite`)

```tableau
// Nb Règles Total (dernier run)
COUNTD(IF [Dernier_Run] THEN [Regle] END)

// Nb Règles OK (dernier run)
SUM(IF [Dernier_Run] AND [Resultat_Ok] THEN 1 ELSE 0 END)

// % Règles OK
SUM(IF [Dernier_Run] AND [Resultat_Ok] THEN 1 ELSE 0 END)
/ COUNTD(IF [Dernier_Run] THEN [Regle] END)

// Nb Bloquants KO (dernier run)
SUM(IF [Dernier_Run] AND [Severite] = "BLOQUANT" AND NOT [Resultat_Ok] THEN 1 ELSE 0 END)
```

---

## Mise en forme conditionnelle recommandée

| Champ | Couleur | Condition |
|-------|---------|-----------|
| `Statut` | Vert | "OK" |
| `Statut` | Rouge | "BLOQUANT" |
| `Statut` | Orange | "WARNING" |
| `Eligible_Prime` | Bleu | True |
| `Eligible_Prime` | Gris | False |

> Dans Tableau : glisser la mesure ou la dimension sur **Couleur** dans la fiche de repères,
> puis **Modifier les couleurs** pour configurer manuellement.

---

## Feuilles recommandées et visuels

| Feuille | Visuels clés | Source |
|---------|--------------|--------|
| **Vue Globale** | Cartes KPI (coût total, % éligibles), barres groupées par BU | `vue_globale` |
| **Primes Sportives** | Nuage de points (distance vs prime), filtre éligible, paramètre taux | `primes_sportives` |
| **Journées BE** | Histogramme nb activités, ligne de référence à 15, treemap sport | `journees_bienetre` |
| **Activités** | Courbe temporelle mensuelle, barres empilées par sport | `activites_sportives` |
| **Anomalies** | Tableau texte avec couleurs conditionnelles, courbe historique | `anomalies_qualite` |

---

## Actualisation des données

### Option A — Connexion directe PostgreSQL (live, recommandée)
```
Tableau Desktop → Se connecter → PostgreSQL
Serveur   : localhost   Port : 5432
Base      : poc_sport   User : admin   Mot de passe : admin123
```
> Pas de driver tiers requis — connecteur natif Tableau.
> Actualisation 1-clic (F5) ou publication sur Tableau Server avec schedule.

### Option B — Extract Hyper (v2, programmatique)
```powershell
# Générer le .hyper depuis Python
python src/export_tableau.py --format hyper
# → reports/tableau/poc_avantages_sportifs_YYYYMMDD_HHMM.hyper

# Ouvrir dans Tableau Desktop
# Se connecter → Fichiers supplémentaires → *.hyper
```
> Le fichier `.hyper` est le format columnar natif Tableau.
> Kestra peut déclencher la régénération automatiquement via l'API Flask.

### Option C — CSV (v1, fallback)
```powershell
python src/export_tableau.py
# → reports/tableau/*.csv (7 fichiers, UTF-8 BOM)
```

### Actualiser après un run de pipeline
```powershell
# 1. Relancer l'ETL Gold si les paramètres ont changé
python scripts/run_round3.py --params-version v2.0_taux7pct

# 2a. Re-générer le .hyper (v2 — Tableau actualise automatiquement si ouvert)
python src/export_tableau.py --format hyper

# 2b. Ou re-exporter en CSV (v1 — puis Ctrl+F5 dans Tableau)
python src/export_tableau.py
```

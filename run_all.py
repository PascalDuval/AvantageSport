#!/usr/bin/env python3
"""
run_all.py — Pipeline complet POC Avantages Sportifs (R1 → R5)

Script interactif qui :
  1. Collecte les paramètres métier et Monte Carlo en début de session
  2. Enchaîne les rounds R1 → R5 avec affichage des résultats
  3. Lance les tests pytest par round avec affichage PASSED / FAILED
  4. Affiche des aperçus de données à chaque étape et un bilan final

Usage :
    conda activate datascience2
    python run_all.py

Prérequis :
    - Docker PostgreSQL démarré : docker compose -f docker/docker-compose.postgres.yml up -d
    - Fichiers XLSX présents dans data/raw/
"""

import os
import sys
import subprocess
import tempfile
import time
from datetime import datetime
from pathlib import Path

# Fix encodage console Windows (cp1252 → utf-8)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

ROOT  = Path(__file__).resolve().parent
WIDTH = 65
SEP_H = "═" * WIDTH
SEP_L = "─" * WIDTH


# ════════════════════════════════════════════════════════════════════
# Affichage
# ════════════════════════════════════════════════════════════════════

def banner(title: str, sub: str = "") -> None:
    print(f"\n{'#' * WIDTH}")
    print(f"#  {title}")
    if sub:
        print(f"#  {sub}")
    print(f"{'#' * WIDTH}")
    sys.stdout.flush()


def section(label: str) -> None:
    print(f"\n{SEP_H}")
    print(f"  {label}")
    print(f"{SEP_H}")
    sys.stdout.flush()


def subsect(label: str) -> None:
    print(f"\n{SEP_L}")
    print(f"  {label}")
    print(f"{SEP_L}")
    sys.stdout.flush()


def ask(prompt: str, default, cast=str):
    """Saisie interactive avec valeur par défaut."""
    try:
        raw = input(f"  {prompt} [{default}] : ").strip().replace("﻿", "")
        if not raw:
            return default
        return cast(raw)
    except (EOFError, KeyboardInterrupt):
        print(f"\n  → Valeur par défaut retenue : {default}")
        return default
    except ValueError:
        print(f"  Valeur invalide → défaut retenu : {default}")
        return default


def pause(msg: str = "Appuyez sur Entrée pour continuer...") -> None:
    try:
        input(f"\n  ↵  {msg}")
    except (EOFError, KeyboardInterrupt):
        pass


# ════════════════════════════════════════════════════════════════════
# Exécution de scripts (sortie visible dans le terminal)
# ════════════════════════════════════════════════════════════════════

def run_script(rel_path: str, extra_args: list = None, env: dict = None) -> bool:
    """Lance un script Python ; sa sortie coule vers le terminal en temps réel."""
    cmd = [sys.executable, str(ROOT / rel_path)] + (extra_args or [])
    res = subprocess.run(cmd, env=env, cwd=str(ROOT))
    return res.returncode == 0


# ════════════════════════════════════════════════════════════════════
# Snippets Python via fichier temporaire (évite les problèmes
# d'échappement Windows avec python -c "...")
# ════════════════════════════════════════════════════════════════════

def run_snippet(code: str, env: dict) -> subprocess.CompletedProcess:
    """Exécute un snippet Python via un fichier temporaire."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".py", delete=False, encoding="utf-8"
    ) as f:
        f.write(code)
        tmp = f.name
    try:
        return subprocess.run(
            [sys.executable, tmp],
            env=env, cwd=str(ROOT),
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass


# ════════════════════════════════════════════════════════════════════
# Vérification PostgreSQL
# ════════════════════════════════════════════════════════════════════

def check_postgres(env: dict) -> bool:
    code = f"""
import sys
sys.path.insert(0, r"{ROOT / 'src'}")
from database import ping
sys.exit(0 if ping(max_retries=3, delay=2) else 1)
"""
    return run_snippet(code, env).returncode == 0


# ════════════════════════════════════════════════════════════════════
# Mise à jour table config PostgreSQL (avant Round 3)
# ════════════════════════════════════════════════════════════════════

def update_db_config(params: dict, env: dict) -> bool:
    """Injecte les paramètres métier dans la table config de la base."""
    code = f"""
import sys
sys.path.insert(0, r"{ROOT / 'src'}")
from database import get_connection
mapping = {params!r}
with get_connection() as conn:
    with conn.cursor() as cur:
        for cle, val in mapping.items():
            cur.execute(
                "INSERT INTO config (cle, valeur) VALUES (%s, %s) "
                "ON CONFLICT (cle) DO UPDATE SET valeur = EXCLUDED.valeur",
                (cle, str(val))
            )
print("OK")
"""
    res = run_snippet(code, env)
    if res.returncode == 0:
        print("  ✅  Table config PostgreSQL mise à jour avec les paramètres saisis")
        return True
    print(f"  ⚠️   Mise à jour config échouée : {res.stderr.strip()[:200]}")
    return False


# ════════════════════════════════════════════════════════════════════
# Requêtes SQL rapides
# ════════════════════════════════════════════════════════════════════

def db_query(sql: str, env: dict) -> list:
    """Exécute une requête SQL et retourne les lignes comme listes de chaînes."""
    code = f"""
import sys
sys.path.insert(0, r"{ROOT / 'src'}")
from database import get_connection
try:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute({sql!r})
            for row in cur.fetchall():
                print("|".join(str(x) if x is not None else "" for x in row))
except Exception as e:
    sys.stderr.write(str(e))
    sys.exit(1)
"""
    res = run_snippet(code, env)
    if res.returncode == 0 and res.stdout.strip():
        return [line.split("|") for line in res.stdout.strip().split("\n")]
    return []


# ════════════════════════════════════════════════════════════════════
# Aperçus intermédiaires après chaque round
# ════════════════════════════════════════════════════════════════════

def apercu_r1(env: dict) -> None:
    subsect("APERÇU — Round 1 : strava_activities (Monte Carlo 2025)")

    sports = db_query("""
        SELECT sport_type, COUNT(*) nb, ROUND(AVG(duree_s)/60.0, 1) dur_min
        FROM strava_activities
        GROUP BY sport_type ORDER BY nb DESC LIMIT 8
    """, env)
    if sports:
        print(f"\n  {'Sport':<24} {'Activités':>10} {'Durée moy (min)':>16}")
        print(f"  {'─'*52}")
        for r in sports:
            print(f"  {r[0]:<24} {r[1]:>10} {r[2]:>16}")

    totals = db_query("""
        SELECT COUNT(*) total, COUNT(DISTINCT employee_id) athletes,
               COUNT(DISTINCT sport_type) sports
        FROM strava_activities
    """, env)
    if totals:
        t = totals[0]
        print(f"\n  → {t[0]} activités  |  {t[1]} athlètes distincts  |  {t[2]} sports")

    top5 = db_query("""
        SELECT employee_id, COUNT(*) nb FROM strava_activities
        GROUP BY employee_id ORDER BY nb DESC LIMIT 5
    """, env)
    if top5:
        print(f"\n  Top 5 athlètes (activités annuelles) :")
        for i, r in enumerate(top5, 1):
            print(f"     {i}. {r[0]:<14} {r[1]} activités")


def apercu_r2(env: dict) -> None:
    subsect("APERÇU — Round 2 : employees (Silver)")

    rows = db_query("""
        SELECT COUNT(*) total,
               SUM(CASE WHEN eligible_prime THEN 1 ELSE 0 END) eligibles,
               SUM(CASE WHEN sport_declare IS NOT NULL THEN 1 ELSE 0 END) sportifs,
               ROUND(AVG(distance_bureau_km)::NUMERIC, 1) dist_moy
        FROM employees
    """, env)
    if rows:
        r = rows[0]
        print(f"  Total salariés      : {r[0]}")
        print(f"  Avec sport déclaré  : {r[2]}")
        print(f"  Éligibles prime     : {r[1]}")
        print(f"  Distance moy bureau : {r[3]} km")

    sample = db_query("""
        SELECT employee_id, sport_declare,
               ROUND(distance_bureau_km::NUMERIC, 2), mode_transport
        FROM employees WHERE eligible_prime = true
        ORDER BY distance_bureau_km LIMIT 5
    """, env)
    if sample:
        print(f"\n  5 premiers éligibles prime (par distance bureau) :")
        print(f"  {'ID':<12} {'Sport':<20} {'Dist (km)':>10} {'Mode':>12}")
        print(f"  {'─'*57}")
        for r in sample:
            print(f"  {r[0]:<12} {(r[1] or 'N/A'):<20} {r[2]:>10} {(r[3] or ''):>12}")


def apercu_r3(env: dict, taux: float, min_be: int, nb_be: int) -> None:
    subsect("APERÇU — Round 3 : avantages_calcules (Gold)")

    rows = db_query("""
        SELECT COUNT(*) total,
               SUM(CASE WHEN eligible_prime THEN 1 ELSE 0 END) nb_prime,
               ROUND(SUM(montant_prime)::NUMERIC, 2) cout,
               SUM(CASE WHEN eligible_jours_be THEN 1 ELSE 0 END) nb_be
        FROM avantages_calcules
    """, env)
    if rows:
        r = rows[0]
        total = max(int(r[0] or 1), 1)
        pct_p = int(r[1] or 0) * 100 // total
        pct_b = int(r[3] or 0) * 100 // total
        print(f"  Paramètres appliqués :")
        print(f"     Taux prime  = {taux*100:.1f}%   |   Seuil BE = ≥{min_be} activités/an → {nb_be} jours")
        print(f"\n  Résultats :")
        print(f"     Salariés calculés   : {r[0]}")
        print(f"     Éligibles prime     : {r[1]} ({pct_p} %)")
        print(f"     Coût total primes   : {float(r[2] or 0):,.2f} €")
        print(f"     Éligibles jours BE  : {r[3]} ({pct_b} %)")

    top5 = db_query("""
        SELECT a.employee_id, e.sport_declare, a.montant_prime, a.nb_jours_bienetre
        FROM avantages_calcules a
        JOIN employees e ON a.employee_id = e.employee_id
        WHERE a.eligible_prime ORDER BY a.montant_prime DESC LIMIT 5
    """, env)
    if top5:
        print(f"\n  Top 5 primes (montant le plus élevé) :")
        print(f"  {'ID':<12} {'Sport':<18} {'Prime (€)':>12} {'Jours BE':>9}")
        print(f"  {'─'*55}")
        for r in top5:
            prime = f"{float(r[2]):,.2f}" if r[2] else "0.00"
            print(f"  {r[0]:<12} {(r[1] or 'N/A'):<18} {prime:>12} {r[3]:>9}")

    qc = db_query("""
        SELECT COUNT(*),
               SUM(CASE WHEN resultat THEN 1 ELSE 0 END)
        FROM data_quality_results
        WHERE run_at >= NOW() - INTERVAL '2 hours'
    """, env)
    if qc and qc[0][0]:
        print(f"\n  Contrôles qualité (dernier run) : {qc[0][1]}/{qc[0][0]} règles OK")


# ════════════════════════════════════════════════════════════════════
# Tests pytest + affichage des résultats
# ════════════════════════════════════════════════════════════════════

def run_tests(test_file: str, env: dict) -> dict:
    """Lance pytest sur un fichier ; retourne les résultats parsés."""
    cmd = [
        sys.executable, "-m", "pytest",
        str(ROOT / test_file),
        "-v", "--tb=short", "--no-header", "--color=no",
    ]
    res = subprocess.run(
        cmd, capture_output=True, text=True, encoding="utf-8",
        env=env, cwd=str(ROOT),
    )
    output = res.stdout + res.stderr

    passed: list[str] = []
    failed: list[str] = []
    errors: list[str] = []
    failure_details: dict[str, list[str]] = {}
    current_fail: str | None = None

    for line in output.split("\n"):
        s = line.rstrip()

        if " PASSED" in s and "::" in s:
            name = s.split(" PASSED")[0].strip()
            passed.append(name)
            current_fail = None

        elif " FAILED" in s and "::" in s:
            name = s.split(" FAILED")[0].strip()
            failed.append(name)
            current_fail = name
            failure_details[name] = []

        elif " ERROR" in s and "::" in s:
            name = s.split(" ERROR")[0].strip()
            errors.append(name)

        # Collecter les lignes d'erreur pour le test en cours
        if current_fail is not None and s.lstrip().startswith("E "):
            failure_details[current_fail].append(s.lstrip()[2:].strip())

    return {
        "passed": passed,
        "failed": failed,
        "errors": errors,
        "failure_details": failure_details,
        "returncode": res.returncode,
    }


def display_tests(label: str, res: dict) -> bool:
    """Affiche les résultats de tests PASSED/FAILED. Retourne True si tous passés."""
    nb_ok  = len(res["passed"])
    nb_ko  = len(res["failed"]) + len(res["errors"])
    total  = nb_ok + nb_ko
    icon_h = "✅" if nb_ko == 0 else "❌"

    subsect(f"TESTS {label}  ─  {icon_h}  {nb_ok}/{total} passés")

    for t in res["passed"]:
        short = t.split("::")[-1] if "::" in t else t
        print(f"  ✅ PASSED   {short}")

    for t in res["failed"]:
        short = t.split("::")[-1] if "::" in t else t
        print(f"  ❌ FAILED   {short}")
        for dl in res["failure_details"].get(t, [])[-3:]:
            print(f"             ↳ {dl}")

    for t in res["errors"]:
        short = t.split("::")[-1] if "::" in t else t
        print(f"  💥 ERROR    {short}")

    print()
    if nb_ko == 0:
        print(f"  ✅  Tous les tests {label} passés !")
    else:
        print(f"  ⚠️   {nb_ko} test(s) en échec — voir détails ci-dessus")

    sys.stdout.flush()
    return nb_ko == 0


# ════════════════════════════════════════════════════════════════════
# Flask (Round 4)
# ════════════════════════════════════════════════════════════════════

def is_flask_up() -> bool:
    """Vérifie si Flask répond sur localhost:5001."""
    try:
        import urllib.request
        urllib.request.urlopen("http://localhost:5001/health", timeout=2)
        return True
    except Exception:
        return False


def start_flask(env: dict):
    """Démarre Flask en arrière-plan et attend qu'il soit prêt (max 12s)."""
    print("  ▶  Démarrage Flask en arrière-plan (src/flask_entry.py)...")
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "src" / "flask_entry.py")],
        env=env, cwd=str(ROOT),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    for _ in range(24):
        time.sleep(0.5)
        if is_flask_up():
            print("  ✅  Flask prêt sur http://localhost:5001")
            return proc
    print("  ⚠️   Flask ne répond pas après 12s — certains tests R4 peuvent échouer")
    return proc


# ════════════════════════════════════════════════════════════════════
# Bilan global final
# ════════════════════════════════════════════════════════════════════

def bilan_global(summary: dict, env: dict, elapsed: float) -> None:
    section("BILAN GLOBAL — POC AVANTAGES SPORTIFS")

    # Résumé des tests par round
    total_p = total_f = 0
    print("\n  RÉSUMÉ DES TESTS :")
    for rnd, res in summary.items():
        nb_p = len(res["passed"])
        nb_f = len(res["failed"]) + len(res["errors"])
        total_p += nb_p
        total_f += nb_f
        icon = "✅" if nb_f == 0 else "❌"
        print(f"  {icon}  {rnd:<10}  {nb_p:>3} passés   {nb_f:>2} échoués")

    total_all = total_p + total_f
    pct = total_p * 100 // max(total_all, 1)
    print(f"\n  Total : {total_p}/{total_all} tests passés ({pct} %)")

    # Statistiques finales depuis la base
    stats = db_query("""
        SELECT
            (SELECT COUNT(*) FROM strava_activities),
            (SELECT COUNT(DISTINCT employee_id) FROM strava_activities),
            (SELECT COUNT(*) FROM employees),
            (SELECT COALESCE(SUM(CASE WHEN eligible_prime THEN 1 ELSE 0 END), 0)
             FROM avantages_calcules),
            (SELECT COALESCE(ROUND(SUM(montant_prime)::NUMERIC, 2), 0)
             FROM avantages_calcules),
            (SELECT COALESCE(SUM(CASE WHEN eligible_jours_be THEN 1 ELSE 0 END), 0)
             FROM avantages_calcules),
            (SELECT COUNT(*) FROM data_quality_results
             WHERE run_at >= NOW() - INTERVAL '3 hours'),
            (SELECT COALESCE(SUM(CASE WHEN resultat THEN 1 ELSE 0 END), 0)
             FROM data_quality_results WHERE run_at >= NOW() - INTERVAL '3 hours')
    """, env)

    if stats and stats[0][0]:
        s = stats[0]
        cout = f"{float(s[4]):,.2f}" if s[4] else "0.00"
        print(f"""
  ┌───────────────────────────────────────────────────────────┐
  │  DONNÉES                                                  │
  │  Activités simulées 2025      : {s[0]:<6}                   │
  │  Athlètes distincts           : {s[1]:<6}                   │
  │  Salariés Silver (normalisés) : {s[2]:<6}                   │
  ├───────────────────────────────────────────────────────────┤
  │  AVANTAGES CALCULÉS                                       │
  │  Éligibles prime sportive     : {s[3]:<6}                   │
  │  Coût total primes            : {cout:<14} €             │
  │  Éligibles jours bien-être    : {s[5]:<6}                   │
  ├───────────────────────────────────────────────────────────┤
  │  QUALITÉ                                                  │
  │  Règles qualité OK            : {s[7]}/{s[6]}                     │
  └───────────────────────────────────────────────────────────┘""")

    print(f"\n  ⏱  Durée totale : {elapsed:.0f}s  ({elapsed/60:.1f} min)")

    if total_f == 0:
        print(f"\n  🎉  Pipeline R1→R5 complet — TOUS LES TESTS PASSÉS !")
    else:
        print(f"\n  ⚠️   Pipeline terminé avec {total_f} test(s) en échec")

    print(f"\n  📁  Exports Tableau   : {ROOT / 'reports' / 'tableau'}")
    print(f"  📄  Rapport SODA      : {ROOT / 'reports' / 'soda_quality_report.html'}")
    print(f"  🔗  pgAdmin           : http://localhost:5050")
    print(f"  🔗  Flask             : http://localhost:5001")
    print()


# ════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ════════════════════════════════════════════════════════════════════

def main() -> None:
    start = datetime.now()

    banner(
        "POC Avantages Sportifs — Pipeline complet R1 → R5",
        f"Démarrage : {start.strftime('%Y-%m-%d  %H:%M:%S')}",
    )

    # ── PARAMÈTRES MÉTIER ─────────────────────────────────────────────
    section("PARAMÈTRES MÉTIER")
    print("  Appuyez sur Entrée pour garder la valeur entre crochets.\n")

    TAUX_PRIME       = ask("TAUX_PRIME       — taux prime sportive", 0.05, float)
    SEUIL_MARCHE_KM  = ask("SEUIL_MARCHE_KM  — distance max marche/running (km)", 15.0, float)
    SEUIL_VELO_KM    = ask("SEUIL_VELO_KM    — distance max vélo/trottinette (km)", 25.0, float)
    MIN_ACTIVITES_BE = ask("MIN_ACTIVITES_BE — activités/an minimales pour jours BE", 15, int)
    NB_JOURS_BE      = ask("NB_JOURS_BE      — nombre de jours bien-être accordés", 5, int)

    # ── MONTE CARLO ───────────────────────────────────────────────────
    print(f"\n{SEP_L}")
    print("  MONTE CARLO — fourchette d'activités simulées/mois/salarié (Round 1)")
    print(f"{SEP_L}\n")

    MC_MIN = ask("Min activités/mois/salarié", 0, int)
    MC_MAX = ask("Max activités/mois/salarié", 4, int)
    if MC_MAX < MC_MIN:
        print("  ⚠  Max < Min → échange automatique")
        MC_MIN, MC_MAX = MC_MAX, MC_MIN

    print(f"""
  ✅ Paramètres confirmés :
     TAUX_PRIME        = {TAUX_PRIME}  ({TAUX_PRIME * 100:.1f} %)
     SEUIL_MARCHE_KM   = {SEUIL_MARCHE_KM} km
     SEUIL_VELO_KM     = {SEUIL_VELO_KM} km
     MIN_ACTIVITES_BE  = {MIN_ACTIVITES_BE} activités/an
     NB_JOURS_BE       = {NB_JOURS_BE} jours
     Monte Carlo       = {MC_MIN} à {MC_MAX} activités/mois/salarié""")

    # ── Environnement pour les sous-processus ─────────────────────────
    ENV: dict[str, str] = {
        **os.environ,
        "PYTHONUTF8":       "1",        # force UTF-8 dans tous les sous-processus
        "TAUX_PRIME":       str(TAUX_PRIME),
        "SEUIL_MARCHE_KM":  str(SEUIL_MARCHE_KM),
        "SEUIL_VELO_KM":    str(SEUIL_VELO_KM),
        "MIN_ACTIVITES_BE": str(MIN_ACTIVITES_BE),
        "NB_JOURS_BE":      str(NB_JOURS_BE),
        "STRAVA_MIN_ACTS":  str(MC_MIN),
        "STRAVA_MAX_ACTS":  str(MC_MAX),
    }

    # ── Vérification PostgreSQL avant de démarrer ─────────────────────
    print(f"\n  Vérification PostgreSQL...", end=" ", flush=True)
    if not check_postgres(ENV):
        print("❌")
        print("\n  ⛔  PostgreSQL non accessible.")
        print("     Démarrez Docker :")
        print("     docker compose -f docker/docker-compose.postgres.yml up -d")
        sys.exit(1)
    print("✅")

    pause("Tout est prêt. Entrée pour lancer le pipeline R1 → R5...")

    summary: dict[str, dict] = {}
    flask_proc = None

    try:
        # ─────────────────────────────────────────────────────────────
        # ROUND 1 — Infrastructure PostgreSQL + Monte Carlo Strava
        # ─────────────────────────────────────────────────────────────
        section(f"ROUND 1/5 — Infra PostgreSQL + Monte Carlo ({MC_MIN}–{MC_MAX} act/mois)")
        run_script("scripts/run_round1.py", env=ENV)
        apercu_r1(ENV)
        r1 = run_tests("tests/test_round1.py", ENV)
        display_tests("Round 1", r1)
        summary["Round 1"] = r1
        pause("Round 1 terminé — Entrée pour Round 2...")

        # ─────────────────────────────────────────────────────────────
        # ROUND 2 — Ingestion XLSX + ETL Silver + Google Maps
        # ─────────────────────────────────────────────────────────────
        section("ROUND 2/5 — Ingestion XLSX + ETL Silver + Google Maps")
        run_script("scripts/run_round2.py", env=ENV)
        apercu_r2(ENV)
        r2 = run_tests("tests/test_round2.py", ENV)
        display_tests("Round 2", r2)
        summary["Round 2"] = r2
        pause("Round 2 terminé — Entrée pour Round 3...")

        # Injection des paramètres dans la table config avant etl_gold
        subsect("Injection des paramètres dans la table config PostgreSQL")
        update_db_config(
            {
                "taux_prime":        str(TAUX_PRIME),
                "seuil_marche_km":   str(SEUIL_MARCHE_KM),
                "seuil_velo_km":     str(SEUIL_VELO_KM),
                "min_activites_be":  str(MIN_ACTIVITES_BE),
                "nb_jours_bienetre": str(NB_JOURS_BE),
            },
            ENV,
        )

        # ─────────────────────────────────────────────────────────────
        # ROUND 3 — Quality Check SODA + ETL Gold
        # ─────────────────────────────────────────────────────────────
        section("ROUND 3/5 — Quality Check SODA + ETL Gold")
        run_script("scripts/run_round3.py", env=ENV)
        apercu_r3(ENV, TAUX_PRIME, MIN_ACTIVITES_BE, NB_JOURS_BE)
        r3 = run_tests("tests/test_round3.py", ENV)
        display_tests("Round 3", r3)
        summary["Round 3"] = r3
        pause("Round 3 terminé — Entrée pour Round 4...")

        # ─────────────────────────────────────────────────────────────
        # ROUND 4 — Kestra + Flask + Slack
        # ─────────────────────────────────────────────────────────────
        section("ROUND 4/5 — Orchestration Kestra + Flask + Slack")

        if is_flask_up():
            print("  ℹ️   Flask déjà actif sur :5001")
        else:
            flask_proc = start_flask(ENV)

        run_script("scripts/run_round4.py", env=ENV)
        r4 = run_tests("tests/test_round4.py", ENV)
        display_tests("Round 4", r4)
        summary["Round 4"] = r4
        pause("Round 4 terminé — Entrée pour Round 5...")

        # ─────────────────────────────────────────────────────────────
        # ROUND 5 — Export Tableau + Rapport final
        # ─────────────────────────────────────────────────────────────
        section("ROUND 5/5 — Export Tableau + Rapport final")
        run_script("scripts/run_round5.py", ["--format", "csv"], env=ENV)
        r5 = run_tests("tests/test_round5.py", ENV)
        display_tests("Round 5", r5)
        summary["Round 5"] = r5

        # ─────────────────────────────────────────────────────────────
        # BILAN FINAL
        # ─────────────────────────────────────────────────────────────
        elapsed = (datetime.now() - start).total_seconds()
        bilan_global(summary, ENV, elapsed)

    finally:
        if flask_proc is not None:
            flask_proc.terminate()
            print("\n  🛑  Flask arrêté (démarré automatiquement par run_all.py)")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  ⚠️   Pipeline interrompu (Ctrl+C)")
        sys.exit(1)

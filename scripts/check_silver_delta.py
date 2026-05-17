"""
Vérification DuckDB — Delta Lake Silver

Script mentionné dans la section 5.7 du README général.
Vérifie que la couche Silver employees est lisible et cohérente.

Usage:
    python scripts/check_silver_delta.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def main():
    import duckdb
    from config import BRONZE_DIR

    silver_path = str(BRONZE_DIR.parent / "silver" / "employees")

    if not Path(silver_path).exists():
        print(f"❌ Delta Lake Silver non trouvé : {silver_path}")
        print("   Lancez d'abord : python scripts/run_round2.py")
        sys.exit(1)

    print(f"🦆 DuckDB — lecture Delta Lake Silver")
    print(f"   Chemin : {silver_path}")
    print()

    conn = duckdb.connect()
    conn.execute("INSTALL delta; LOAD delta;")

    # Résultat observé dans le README : nb_total=161, eligibles=68, sportifs=161, dist_moy_km=7.1
    df = conn.execute(f"""
        SELECT
            COUNT(*)                                            AS nb_total,
            SUM(CASE WHEN eligible_prime THEN 1 ELSE 0 END)    AS eligibles,
            SUM(CASE WHEN sport_norm IS NOT NULL THEN 1 ELSE 0 END) AS sportifs,
            ROUND(AVG(distance_km)::DECIMAL, 1)                AS dist_moy_km
        FROM delta_scan('{silver_path}')
    """).fetchdf()

    print("📊 Résumé Silver employees :")
    for col, val in df.iloc[0].items():
        print(f"   {col:<20} : {val}")

    # Par mode de déplacement
    df_modes = conn.execute(f"""
        SELECT
            mode_norm,
            COUNT(*) AS nb,
            SUM(CASE WHEN eligible_prime THEN 1 ELSE 0 END) AS eligibles,
            ROUND(AVG(distance_km)::DECIMAL, 1) AS dist_moy
        FROM delta_scan('{silver_path}')
        GROUP BY mode_norm ORDER BY nb DESC
    """).fetchdf()

    print("\n📋 Par mode de déplacement :")
    print(df_modes.to_string(index=False))
    print(f"\n   ℹ️  dist_moy = NULL pour TC/véhicule (normal — distance non calculée)")

    # Vérifier que le cache ne contient que walking/bicycling
    print("\n🔍 Vérification gmaps_cache (doit contenir UNIQUEMENT walking et bicycling) :")
    try:
        from database import get_connection
        with get_connection() as db_conn:
            with db_conn.cursor() as cur:
                cur.execute("""
                    SELECT mode_transport, COUNT(*) AS nb
                    FROM gmaps_cache GROUP BY mode_transport ORDER BY mode_transport
                """)
                rows = cur.fetchall()
        for r in rows:
            icon = "✅" if r[0] in ("walking", "bicycling") else "🔴 ANORMAL"
            print(f"   {icon} {r[0]:<15} {r[1]} entrées")
    except Exception as e:
        print(f"   ⚠️  Impossible de lire gmaps_cache : {e}")

    conn.close()
    print(f"\n✅ Delta Lake Silver lisible — {silver_path}")


if __name__ == "__main__":
    main()

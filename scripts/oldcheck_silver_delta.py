import duckdb
conn = duckdb.connect()
conn.execute("INSTALL delta; LOAD delta;")

df = conn.execute("""
    SELECT
        COUNT(*) as nb_total,
        SUM(CASE WHEN eligible_prime THEN 1 ELSE 0 END) as eligibles,
        SUM(CASE WHEN sport_norm IS NOT NULL THEN 1 ELSE 0 END) as sportifs,
        ROUND(AVG(distance_km)::DECIMAL, 1) as dist_moy_km
    FROM delta_scan('data/delta/silver/employees')
""").fetchdf()
print(df)
conn.close()

# scripts/04_reload_stats.py
# CILJ: Briše stare ALL statistike, učitava per-season podatke
# POKRENI: cd scripts && python 04_reload_stats.py

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from utils import get_connection, safe_int, safe_float, clean_percentage

# =============================================================
# EXTRACT
# =============================================================
print("\n" + "="*60)
print("EXTRACT — učitavanje per-season CSV-ova")
print("="*60)

base = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

df_players = pd.read_csv(os.path.join(base, "player_stats_per_season.csv"))
df_teams   = pd.read_csv(os.path.join(base, "team_stats_per_season.csv"))

print(f"✅ Player stats: {len(df_players)} redova")
print(f"✅ Team stats:   {len(df_teams)} redova")

# =============================================================
# KONEKCIJA + CLEANUP
# =============================================================
conn = get_connection()
if conn is None:
    exit()
cursor = conn.cursor()

print("\nBrišem stare ALL podatke...")
cursor.execute("DELETE FROM fact_player_stats WHERE season_code = 'ALL'")
cursor.execute("DELETE FROM fact_team_stats   WHERE season_code = 'ALL'")
conn.commit()
print("✅ Stari podaci obrisani")

# =============================================================
# LOOKUP MAPE
# =============================================================
cursor.execute("SELECT season_id, season_code FROM dim_season")
season_map = {row[1]: row[0] for row in cursor.fetchall()}

cursor.execute("SELECT player_id, player_code FROM dim_player")
player_map = {row[1]: row[0] for row in cursor.fetchall()}

cursor.execute("SELECT team_id, team_code FROM dim_team")
team_map = {row[1]: row[0] for row in cursor.fetchall()}

print(f"\nSezone dostupne u bazi: {sorted(season_map.keys())}")

# =============================================================
# LOAD — fact_player_stats
# =============================================================
print("\n" + "="*60)
print("LOAD: fact_player_stats")
print("="*60)

sql_pstats = """
    INSERT INTO fact_player_stats (
        player_id, season_id, team_id,
        player_code, team_code, season_code,
        games_played, games_started, minutes_played, points_scored,
        two_pointers_made, two_pointers_attempted, two_pointers_pct,
        three_pointers_made, three_pointers_attempted, three_pointers_pct,
        free_throws_made, free_throws_attempted, free_throws_pct,
        offensive_rebounds, defensive_rebounds, total_rebounds,
        assists, steals, turnovers, blocks, blocks_against,
        fouls_committed, fouls_drawn, pir,
        true_shooting_pct, assist_turnover_ratio, rebound_rate
    ) VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s
    )
    ON DUPLICATE KEY UPDATE
        games_played  = VALUES(games_played),
        points_scored = VALUES(points_scored),
        pir           = VALUES(pir)
"""

count  = 0
errors = 0
skipped = set()

for _, row in df_players.iterrows():
    try:
        player_code = str(row["player.code"])
        team_code   = str(row["player.team.code"])
        season_code = str(row["season_code"])
        season_id   = season_map.get(season_code)

        if season_id is None:
            skipped.add(season_code)
            continue

        # Dodaj igrača ako nije u dim_player
        if player_code not in player_map:
            cursor.execute("""
                INSERT IGNORE INTO dim_player
                    (player_code, player_name, player_age, image_url)
                VALUES (%s, %s, %s, %s)
            """, (
                player_code,
                str(row["player.name"]),
                safe_int(row.get("player.age")),
                str(row["player.imageUrl"]) if pd.notna(row.get("player.imageUrl")) else None
            ))
            conn.commit()
            cursor.execute(
                "SELECT player_id FROM dim_player WHERE player_code = %s",
                (player_code,)
            )
            result = cursor.fetchone()
            if result:
                player_map[player_code] = result[0]

        pts   = safe_float(row["pointsScored"])
        fga   = safe_float(row["twoPointersAttempted"])
        tpa   = safe_float(row["threePointersAttempted"])
        fta   = safe_float(row["freeThrowsAttempted"])
        ast   = safe_float(row["assists"])
        tov   = safe_float(row["turnovers"])
        reb   = safe_float(row["totalRebounds"])
        min_p = safe_float(row["minutesPlayed"])

        denom    = 2 * ((fga or 0) + (tpa or 0) + 0.44 * (fta or 0))
        ts_pct   = (pts / denom) if (pts and denom > 0) else None
        ast_to   = (ast / tov)   if (ast and tov and tov > 0) else None
        reb_rate = (reb / min_p) if (reb and min_p and min_p > 0) else None

        cursor.execute(sql_pstats, (
            player_map.get(player_code), season_id, team_map.get(team_code),
            player_code, team_code, season_code,
            safe_int(row["gamesPlayed"]),
            safe_int(row["gamesStarted"]),
            min_p, pts,
            safe_float(row["twoPointersMade"]), fga,
            clean_percentage(row["twoPointersPercentage"]),
            safe_float(row["threePointersMade"]), tpa,
            clean_percentage(row["threePointersPercentage"]),
            safe_float(row["freeThrowsMade"]), fta,
            clean_percentage(row["freeThrowsPercentage"]),
            safe_float(row["offensiveRebounds"]),
            safe_float(row["defensiveRebounds"]), reb,
            ast, safe_float(row["steals"]), tov,
            safe_float(row["blocks"]),
            safe_float(row["blocksAgainst"]),
            safe_float(row["foulsCommited"]),
            safe_float(row["foulsDrawn"]),
            safe_float(row["pir"]),
            ts_pct, ast_to, reb_rate
        ))
        count += 1

    except Exception as e:
        errors += 1
        if errors <= 3:
            print(f"  ⚠️  {e}")

conn.commit()
print(f"✅ Ubačeno {count} redova | ⚠️ {errors} grešaka")
if skipped:
    print(f"  ⚠️  Preskočene sezone: {skipped}")

# =============================================================
# LOAD — fact_team_stats
# =============================================================
print("\n" + "="*60)
print("LOAD: fact_team_stats")
print("="*60)

sql_tstats = """
    INSERT INTO fact_team_stats (
        team_id, season_id, team_code, season_code,
        games_played, minutes_played, points_scored,
        two_pointers_made, two_pointers_attempted, two_pointers_pct,
        three_pointers_made, three_pointers_attempted, three_pointers_pct,
        free_throws_made, free_throws_attempted, free_throws_pct,
        offensive_rebounds, defensive_rebounds, total_rebounds,
        assists, steals, turnovers, blocks, blocks_against,
        fouls_committed, fouls_drawn, pir
    ) VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s
    )
    ON DUPLICATE KEY UPDATE
        games_played  = VALUES(games_played),
        points_scored = VALUES(points_scored),
        pir           = VALUES(pir)
"""

count  = 0
errors = 0
skipped = set()

for _, row in df_teams.iterrows():
    try:
        team_code   = str(row["team.code"])
        season_code = str(row["season_code"])
        season_id   = season_map.get(season_code)

        if season_id is None:
            skipped.add(season_code)
            continue

        cursor.execute(sql_tstats, (
            team_map.get(team_code), season_id,
            team_code, season_code,
            safe_int(row["gamesPlayed"]),
            safe_float(row["minutesPlayed"]),
            safe_float(row["pointsScored"]),
            safe_float(row["twoPointersMade"]),
            safe_float(row["twoPointersAttempted"]),
            clean_percentage(row["twoPointersPercentage"]),
            safe_float(row["threePointersMade"]),
            safe_float(row["threePointersAttempted"]),
            clean_percentage(row["threePointersPercentage"]),
            safe_float(row["freeThrowsMade"]),
            safe_float(row["freeThrowsAttempted"]),
            clean_percentage(row["freeThrowsPercentage"]),
            safe_float(row["offensiveRebounds"]),
            safe_float(row["defensiveRebounds"]),
            safe_float(row["totalRebounds"]),
            safe_float(row["assists"]),
            safe_float(row["steals"]),
            safe_float(row["turnovers"]),
            safe_float(row["blocks"]),
            safe_float(row["blocksAgainst"]),
            safe_float(row["foulsCommited"]),
            safe_float(row["foulsDrawn"]),
            safe_float(row["pir"])
        ))
        count += 1

    except Exception as e:
        errors += 1
        if errors <= 3:
            print(f"  ⚠️  {e}")

conn.commit()
print(f"✅ Ubačeno {count} redova | ⚠️ {errors} grešaka")
if skipped:
            print(f"  ⚠️  Preskočene sezone: {skipped}")

# =============================================================
# VERIFIKACIJA
# =============================================================
print("\n" + "="*60)
print("VERIFIKACIJA")
print("="*60)

tables = ["dim_season", "dim_team", "dim_player",
          "fact_games", "fact_player_stats", "fact_team_stats"]

for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    print(f"  {table:<28} {cursor.fetchone()[0]:>6} redova")

print("\nPlayer stats po sezoni:")
cursor.execute("""
    SELECT season_code, COUNT(*) 
    FROM fact_player_stats 
    GROUP BY season_code 
    ORDER BY season_code
""")
for row in cursor.fetchall():
    print(f"  {row[0]}  →  {row[1]} igrača")

print("\nTeam stats po sezoni:")
cursor.execute("""
    SELECT season_code, COUNT(*) 
    FROM fact_team_stats 
    GROUP BY season_code 
    ORDER BY season_code
""")
for row in cursor.fetchall():
    print(f"  {row[0]}  →  {row[1]} timova")

cursor.close()
conn.close()
print("\n✅ Gotovo! Baza je spremna za PowerBI.")
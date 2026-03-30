# =============================================================
# FAJL: 02_etl_pipeline.py
# CILJ: ETL pipeline — čitamo CSV fajlove, punimo MySQL bazu
#
# ETL = Extract → Transform → Load
#   Extract   = čitamo sirove CSV fajlove iz data/raw/
#   Transform = čistimo i transformišemo podatke
#   Load      = ubacujemo u MySQL tabele
#
# POKRENI: python 02_etl_pipeline.py
# =============================================================

import pandas as pd
import mysql.connector
from mysql.connector import Error
import numpy as np
from datetime import datetime, timedelta

# =============================================================
# KONFIGURACIJA BAZE
# Promeni lozinku ako imaš drugačiju!
# =============================================================
DB_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": "admin",       
    "database": "euroleague_db",
    "charset":  "utf8mb4"
}

# =============================================================
# HELPER FUNKCIJE
# =============================================================

def get_connection():
    """Kreira konekciju sa MySQL bazom."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            print(" Konekcija sa bazom uspešna!")
            return conn
    except Error as e:
        print(f" Greška pri konekciji: {e}")
        print("   Proveri lozinku u DB_CONFIG!")
        return None

def clean_percentage(value):
    """
    Konvertuje string procenat u decimalni broj.
    "57.6%" → 0.576
    Ako je već broj ili None, vraća ga nepromenjenog.
    """
    if pd.isna(value) or value is None:
        return None
    if isinstance(value, str):
        try:
            return float(value.replace("%", "").strip()) / 100
        except ValueError:
            return None
    return float(value) / 100 if float(value) > 1 else float(value)

def safe_int(value):
    """Konvertuje vrednost u int, None ako nije moguće."""
    if pd.isna(value) or value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def safe_float(value):
    """Konvertuje vrednost u float, None ako nije moguće."""
    if pd.isna(value) or value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def safe_date(value):
    """Parsira datum string u DATE format za MySQL."""
    if pd.isna(value) or value is None:
        return None
    try:
        # Format iz API-ja: "2022-10-06T20:00:00"
        return datetime.fromisoformat(str(value)[:10]).date()
    except (ValueError, TypeError):
        return None

# =============================================================
# EXTRACT — Učitavamo CSV fajlove
# =============================================================

print("\n" + "=" * 60)
print("EXTRACT — Učitavanje CSV fajlova")
print("=" * 60)

df_results = pd.read_csv("data/raw/game_results_2022_2024.csv")
df_players = pd.read_csv("data/raw/player_stats_2022_2024.csv")
df_teams   = pd.read_csv("data/raw/team_stats_2022_2024.csv")

print(f"✅ game_results:  {len(df_results)} redova")
print(f"✅ player_stats:  {len(df_players)} redova")
print(f"✅ team_stats:    {len(df_teams)} redova")

# =============================================================
# Otvaramo konekciju
# =============================================================
conn = get_connection()
if conn is None:
    exit()

cursor = conn.cursor()

# =============================================================
# LOAD 1 — dim_season
# =============================================================

print("\n" + "=" * 60)
print("LOAD: dim_season")
print("=" * 60)

# Izvlačimo jedinstvene sezone iz game_results
seasons = df_results[["Season", "season.code", "season.name",
                       "season.alias", "season.startDate"]].drop_duplicates()

sql_season = """
    INSERT INTO dim_season
        (season_year, season_code, season_name, season_alias, start_date)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        season_name  = VALUES(season_name),
        season_alias = VALUES(season_alias)
"""

count = 0
for _, row in seasons.iterrows():
    cursor.execute(sql_season, (
        safe_int(row["Season"]),
        str(row["season.code"]),
        str(row["season.name"]),
        str(row["season.alias"]),
        safe_date(row["season.startDate"])
    ))
    count += 1

conn.commit()
print(f"✅ Ubačeno {count} sezona")

# =============================================================
# LOAD 2 — dim_team
# =============================================================

print("\n" + "=" * 60)
print("LOAD: dim_team")
print("=" * 60)

# Timovi se pojavljuju kao home (local) i away (road)
home_teams = df_results[["local.club.code", "local.club.name",
                          "local.club.tvCode",
                          "local.club.images.crest"]].copy()
home_teams.columns = ["code", "name", "tv_code", "image_url"]

away_teams = df_results[["road.club.code", "road.club.name",
                          "road.club.tvCode",
                          "road.club.images.crest"]].copy()
away_teams.columns = ["code", "name", "tv_code", "image_url"]

# Spajamo i uzimamo jedinstvene timove
all_teams = pd.concat([home_teams, away_teams]).drop_duplicates(
    subset=["code"]
)

sql_team = """
    INSERT INTO dim_team (team_code, team_name, team_tv_code, image_url)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        team_name    = VALUES(team_name),
        team_tv_code = VALUES(team_tv_code)
"""

count = 0
for _, row in all_teams.iterrows():
    cursor.execute(sql_team, (
        str(row["code"]),
        str(row["name"]),
        str(row["tv_code"]) if pd.notna(row["tv_code"]) else None,
        str(row["image_url"]) if pd.notna(row["image_url"]) else None
    ))
    count += 1

conn.commit()
print(f"✅ Ubačeno {count} timova")

# =============================================================
# LOAD 3 — dim_player
# =============================================================

print("\n" + "=" * 60)
print("LOAD: dim_player")
print("=" * 60)

sql_player = """
    INSERT INTO dim_player (player_code, player_name, player_age, image_url)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        player_name = VALUES(player_name),
        player_age  = VALUES(player_age)
"""

count = 0
for _, row in df_players.drop_duplicates(subset=["player.code"]).iterrows():
    cursor.execute(sql_player, (
        str(row["player.code"]),
        str(row["player.name"]),
        safe_int(row["player.age"]),
        str(row["player.imageUrl"]) if pd.notna(row["player.imageUrl"]) else None
    ))
    count += 1

conn.commit()
print(f"✅ Ubačeno {count} igrača")

# =============================================================
# LOAD 4 — dim_calendar
# =============================================================

print("\n" + "=" * 60)
print("LOAD: dim_calendar")
print("=" * 60)

# Generišemo sve datume od prve do poslednje utakmice
dates = pd.to_datetime(df_results["date"].str[:10]).dt.date
min_date = min(dates)
max_date = max(dates)

sql_calendar = """
    INSERT INTO dim_calendar
        (full_date, year, month, month_name, day,
         day_of_week, day_name, week_of_year, quarter, is_weekend)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE year = VALUES(year)
"""

current = min_date
count = 0
while current <= max_date:
    dt = datetime.combine(current, datetime.min.time())
    cursor.execute(sql_calendar, (
        current,
        current.year,
        current.month,
        dt.strftime("%B"),           # October, November...
        current.day,
        dt.isoweekday(),             # 1=Monday, 7=Sunday
        dt.strftime("%A"),           # Monday, Tuesday...
        int(dt.strftime("%W")),      # nedelja u godini
        (current.month - 1) // 3 + 1,  # kvartal 1-4
        current.weekday() >= 5       # True ako je vikend
    ))
    current += timedelta(days=1)
    count += 1

conn.commit()
print(f"✅ Ubačeno {count} datuma u dim_calendar")

# =============================================================
# LOAD 5 — fact_games
# =============================================================

print("\n" + "=" * 60)
print("LOAD: fact_games")
print("=" * 60)

# Uzimamo season_id i date_id iz dimenzija
cursor.execute("SELECT season_id, season_code FROM dim_season")
season_map = {row[1]: row[0] for row in cursor.fetchall()}

cursor.execute("SELECT date_id, full_date FROM dim_calendar")
date_map = {str(row[1]): row[0] for row in cursor.fetchall()}

sql_game = """
    INSERT INTO fact_games (
        season_id, date_id, gamecode, season_year, season_code,
        phase, round_number, round_name,
        home_team_code, away_team_code, home_team_name, away_team_name,
        home_score, away_score, home_win, score_diff,
        game_date, game_datetime_utc
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s
    )
    ON DUPLICATE KEY UPDATE
        home_score = VALUES(home_score),
        away_score = VALUES(away_score),
        home_win   = VALUES(home_win)
"""

count = 0
errors = 0
for _, row in df_results.iterrows():
    try:
        game_date_str = str(row["date"])[:10]
        game_date     = safe_date(row["date"])
        season_code   = str(row["season.code"])
        home_score    = safe_int(row["local.score"])
        away_score    = safe_int(row["road.score"])

        # Izračunavamo home_win i score_diff
        home_win   = bool(home_score > away_score) if (
            home_score is not None and away_score is not None) else None
        score_diff = abs(home_score - away_score) if (
            home_score is not None and away_score is not None) else None

        # Parsiramo UTC datetime
        try:
            utc_dt = datetime.fromisoformat(
                str(row["utcDate"]).replace("Z", "+00:00")
            ).replace(tzinfo=None)
        except Exception:
            utc_dt = None

        cursor.execute(sql_game, (
            season_map.get(season_code),
            date_map.get(game_date_str),
            safe_int(row["Gamecode"]),
            safe_int(row["Season"]),
            season_code,
            str(row["Phase"]) if pd.notna(row["Phase"]) else None,
            safe_int(row["Round"]),
            str(row["roundName"]) if pd.notna(row["roundName"]) else None,
            str(row["local.club.code"]),
            str(row["road.club.code"]),
            str(row["local.club.name"]),
            str(row["road.club.name"]),
            home_score,
            away_score,
            home_win,
            score_diff,
            game_date,
            utc_dt
        ))
        count += 1
    except Exception as e:
        errors += 1
        if errors <= 3:  # Prikazujemo prvih 5 grešaka
            print(f"  ⚠️  Red {count}: {e}")

conn.commit()
print(f"✅ Ubačeno {count} utakmica | ⚠️ {errors} grešaka")

# =============================================================
# LOAD 6 — fact_player_stats
# =============================================================

print("\n" + "=" * 60)
print("LOAD: fact_player_stats")
print("=" * 60)

# Napomena: player_stats CSV nema season_code kolonu!
# Dodajemo je ručno — ovaj DataFrame ima podatke za sve sezone
# ali bez oznake koja je koja. Ovo ćemo poboljšati u sledećoj iteraciji.

cursor.execute("SELECT player_id, player_code FROM dim_player")
player_map = {row[1]: row[0] for row in cursor.fetchall()}

cursor.execute("SELECT team_id, team_code FROM dim_team")
team_map = {row[1]: row[0] for row in cursor.fetchall()}

sql_pstats = """
    INSERT INTO fact_player_stats (
        player_id, season_id, team_id,
        player_code, team_code, season_code,
        games_played, games_started, minutes_played,
        points_scored,
        two_pointers_made, two_pointers_attempted, two_pointers_pct,
        three_pointers_made, three_pointers_attempted, three_pointers_pct,
        free_throws_made, free_throws_attempted, free_throws_pct,
        offensive_rebounds, defensive_rebounds, total_rebounds,
        assists, steals, turnovers, blocks, blocks_against,
        fouls_committed, fouls_drawn, pir,
        true_shooting_pct, assist_turnover_ratio, rebound_rate
    ) VALUES (
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        games_played = VALUES(games_played),
        pir          = VALUES(pir)
"""

count = 0
errors = 0
for _, row in df_players.iterrows():
    try:
        player_code = str(row["player.code"])
        team_code   = str(row["player.team.code"])
        # Bez season_code u ovom CSV-u — koristimo placeholder
        season_code = "ALL"

        # Napredne metrike — računamo u ETL-u
        pts = safe_float(row["pointsScored"])
        fga = safe_float(row["twoPointersAttempted"])
        tpa = safe_float(row["threePointersAttempted"])
        fta = safe_float(row["freeThrowsAttempted"])
        ast = safe_float(row["assists"])
        tov = safe_float(row["turnovers"])
        reb = safe_float(row["totalRebounds"])
        min_p = safe_float(row["minutesPlayed"])

        # True Shooting % = PTS / (2 * (FGA + 0.44 * FTA))
        ts_pct = None
        if pts and fga and fta:
            denom = 2 * ((fga + tpa) + 0.44 * fta)
            ts_pct = pts / denom if denom > 0 else None

        # Assist/Turnover ratio
        ast_to = ast / tov if (ast and tov and tov > 0) else None

        # Rebound rate per minute
        reb_rate = reb / min_p if (reb and min_p and min_p > 0) else None

        cursor.execute(sql_pstats, (
            player_map.get(player_code),
            season_map.get(season_code, list(season_map.values())[0]),
            team_map.get(team_code),
            player_code, team_code, season_code,
            safe_int(row["gamesPlayed"]),
            safe_int(row["gamesStarted"]),
            safe_float(row["minutesPlayed"]),
            pts,
            safe_float(row["twoPointersMade"]),
            fga,
            clean_percentage(row["twoPointersPercentage"]),
            safe_float(row["threePointersMade"]),
            tpa,
            clean_percentage(row["threePointersPercentage"]),
            safe_float(row["freeThrowsMade"]),
            fta,
            clean_percentage(row["freeThrowsPercentage"]),
            safe_float(row["offensiveRebounds"]),
            safe_float(row["defensiveRebounds"]),
            safe_float(row["totalRebounds"]),
            ast,
            safe_float(row["steals"]),
            tov,
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
print(f"✅ Ubačeno {count} igrač-statistika | ⚠️ {errors} grešaka")

# =============================================================
# LOAD 7 — fact_team_stats
# =============================================================

print("\n" + "=" * 60)
print("LOAD: fact_team_stats")
print("=" * 60)

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
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        games_played  = VALUES(games_played),
        points_scored = VALUES(points_scored)
"""

count = 0
errors = 0
for _, row in df_teams.iterrows():
    try:
        team_code   = str(row["team.code"])
        season_code = "ALL"

        cursor.execute(sql_tstats, (
            team_map.get(team_code),
            season_map.get(season_code, list(season_map.values())[0]),
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
print(f"✅ Ubačeno {count} tim-statistika | ⚠️ {errors} grešaka")

# =============================================================
# VERIFIKACIJA — Proveravamo koliko ima redova u svakoj tabeli
# =============================================================

print("\n" + "=" * 60)
print("VERIFIKACIJA — Broj redova po tabeli")
print("=" * 60)

tables = ["dim_season", "dim_team", "dim_player", "dim_calendar",
          "fact_games", "fact_player_stats", "fact_team_stats"]

for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"  {table:<25} {count:>6} redova")

cursor.close()
conn.close()

print("\n ETL pipeline završen!")
print("Sada možeš da otvoriš MySQL Workbench i vidiš podatke u tabelama.")
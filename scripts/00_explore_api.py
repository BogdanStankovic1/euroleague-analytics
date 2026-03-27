# =============================================================
# FAJL: scripts/00_explore_api.py
# CILJ: Povući sirove podatke sa Euroleague API-ja
# POKRENI: cd scripts && python 00_explore_api.py
# NAPOMENA: Pokreći samo kada treba osvežiti sirove podatke
# =============================================================

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from euroleague_api.game_stats import GameStats
from euroleague_api.player_stats import PlayerStats
from euroleague_api.team_stats import TeamStats
import pandas as pd
import os

COMPETITION  = "E"
START_SEASON = 2015
END_SEASON   = 2024

game_stats   = GameStats(COMPETITION)
player_stats = PlayerStats(COMPETITION)
team_stats   = TeamStats(COMPETITION)

base = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(base, exist_ok=True)

# =============================================================
# KORAK 1 — Rezultati utakmica (range je OK ovde)
# =============================================================
print("="*60)
print(f"KORAK 1: Rezultati utakmica {START_SEASON}–{END_SEASON}")
print("="*60)

df_results = game_stats.get_game_reports_range_seasons(
    start_season=START_SEASON,
    end_season=END_SEASON
)

print(f"Ukupno utakmica: {len(df_results)}")
df_results.to_csv(os.path.join(base, "game_results.csv"), index=False)
print("✅ Sačuvano: game_results.csv")

# =============================================================
# KORAK 2 — Player stats PO SEZONI (loop, ne range)
# =============================================================
print("\n" + "="*60)
print(f"KORAK 2: Player stats {START_SEASON}–{END_SEASON} (per season)")
print("="*60)

all_player_stats = []
for season in range(START_SEASON, END_SEASON + 1):
    try:
        df_p = player_stats.get_player_stats_single_season(
            endpoint="traditional",
            season=season,
            phase_type_code="RS",
            statistic_mode="PerGame"
        )
        df_p["season_year"] = season
        df_p["season_code"] = f"E{season}"
        all_player_stats.append(df_p)
        print(f"  ✅ {season}: {len(df_p)} igrača")
    except Exception as e:
        print(f"  ⚠️  {season}: {e}")

df_players = pd.concat(all_player_stats, ignore_index=True)
print(f"\nUkupno: {len(df_players)} redova")
df_players.to_csv(os.path.join(base, "player_stats_per_season.csv"), index=False)
print("✅ Sačuvano: player_stats_per_season.csv")

# =============================================================
# KORAK 3 — Team stats PO SEZONI (loop, ne range)
# =============================================================
print("\n" + "="*60)
print(f"KORAK 3: Team stats {START_SEASON}–{END_SEASON} (per season)")
print("="*60)

all_team_stats = []
for season in range(START_SEASON, END_SEASON + 1):
    try:
        df_t = team_stats.get_team_stats_single_season(
            endpoint="traditional",
            season=season,
            phase_type_code="RS",
            statistic_mode="PerGame"
        )
        df_t["season_year"] = season
        df_t["season_code"] = f"E{season}"
        all_team_stats.append(df_t)
        print(f"  ✅ {season}: {len(df_t)} timova")
    except Exception as e:
        print(f"  ⚠️  {season}: {e}")

df_teams = pd.concat(all_team_stats, ignore_index=True)
print(f"\nUkupno: {len(df_teams)} redova")
df_teams.to_csv(os.path.join(base, "team_stats_per_season.csv"), index=False)
print("✅ Sačuvano: team_stats_per_season.csv")

# =============================================================
# SUMMARY
# =============================================================
print("\n" + "="*60)
print("SUMMARY — fajlovi u data/raw/")
print("="*60)
print(f"  game_results.csv            {len(df_results):>6} redova")
print(f"  player_stats_per_season.csv {len(df_players):>6} redova")
print(f"  team_stats_per_season.csv   {len(df_teams):>6} redova")
print("\nSledeći korak: python 02_etl_pipeline.py")
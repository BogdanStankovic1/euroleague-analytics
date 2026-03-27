# =============================================================
# FAJL: 04_api.py
# CILJ: Povući player i team stats PO SEZONI (ne agregirano)
# =============================================================

from euroleague_api.player_stats import PlayerStats
from euroleague_api.team_stats import TeamStats
import pandas as pd
import os

COMPETITION  = "E"
START_SEASON = 2015
END_SEASON   = 2024

player_stats = PlayerStats(COMPETITION)
team_stats   = TeamStats(COMPETITION)

os.makedirs("data/raw", exist_ok=True)

all_player_stats = []
all_team_stats   = []

for season in range(START_SEASON, END_SEASON + 1):
    print(f"Povlačim sezonu {season}...")

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
        print(f"  ✓ Player: {len(df_p)} igrača")
    except Exception as e:
        print(f"  ✗ Player greška za {season}: {e}")

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
        print(f"  ✓ Team: {len(df_t)} timova")
    except Exception as e:
        print(f"  ✗ Team greška za {season}: {e}")

df_players_final = pd.concat(all_player_stats, ignore_index=True)
df_teams_final   = pd.concat(all_team_stats,   ignore_index=True)

print(f"\nPlayer stats ukupno: {len(df_players_final)} redova")
print(f"Team stats ukupno:   {len(df_teams_final)} redova")

df_players_final.to_csv("data/raw/player_stats_per_season.csv", index=False)
df_teams_final.to_csv("data/raw/team_stats_per_season.csv",   index=False)

print("\nSačuvano u data/raw/")
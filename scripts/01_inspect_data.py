

# =============================================================
# FAJL: 01_inspect_data.py
# CILJ: Pregledamo strukture CSV fajlova pre dizajna baze
# POKRENI: python 01_inspect_data.py
# =============================================================
 
import pandas as pd
import os
 
csv_files = {
    "Game Results":     "data/raw/game_results_2022_2024.csv",
    "Game Team Stats":  "data/raw/game_team_stats_2022_2024.csv",
    "Player Stats":     "data/raw/player_stats_2022_2024.csv",
    "Team Stats":       "data/raw/team_stats_2022_2024.csv",
}
 
for name, path in csv_files.items():
    if not os.path.exists(path):
        print(f"\n  Fajl nije pronađen: {path}")
        continue
 
    df = pd.read_csv(path)
 
    print("\n" + "=" * 60)
    print(f"📄 {name}")
    print(f"   Fajl: {path}")
    print("=" * 60)
    print(f"  Redovi:  {len(df)}")
    print(f"  Kolone:  {len(df.columns)}")
    print(f"\n  Kolone i tipovi podataka:")
    for col, dtype in df.dtypes.items():
        # Primer vrednosti iz prve neprazne ćelije
        sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else "N/A"
        print(f"    {col:<35} {str(dtype):<10}  npr: {sample}")
 
    print(f"\n  Prvih 2 reda:")
    print(df.head(2).to_string())
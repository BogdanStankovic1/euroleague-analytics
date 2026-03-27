# =============================================================
# FAJL: scripts/07_prediction_model.py
# CILJ: Predvidjanje broja poena u utakmici — Regression model
# =============================================================

import pandas as pd
import numpy as np
import mysql.connector
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import warnings
warnings.filterwarnings('ignore')

DB_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": "admin",
    "database": "euroleague_db",
    "charset":  "utf8mb4"
}

# =============================================================
# EXTRACT — povuci podatke iz MySQL
# =============================================================
print("="*60)
print("EXTRACT — ucitavanje podataka iz baze")
print("="*60)

conn = mysql.connector.connect(**DB_CONFIG)

# Glavna tabela — fact_games sa sezonskim statistikama timova
query = """
SELECT
    fg.game_id,
    fg.season_code,
    fg.season_year,
    fg.phase,
    fg.round_number,
    fg.home_score,
    fg.away_score,
    fg.score_diff,
    fg.home_win,
    fg.home_team_code,
    fg.away_team_code,

    -- Home tim sezonske statistike
    ht.points_scored        AS home_avg_pts,
    ht.two_pointers_pct     AS home_2pt_pct,
    ht.three_pointers_pct   AS home_3pt_pct,
    ht.free_throws_pct      AS home_ft_pct,
    ht.assists              AS home_avg_ast,
    ht.turnovers            AS home_avg_tov,
    ht.offensive_rebounds   AS home_avg_oreb,
    ht.defensive_rebounds   AS home_avg_dreb,
    ht.pir                  AS home_avg_pir,

    -- Away tim sezonske statistike
    at2.points_scored       AS away_avg_pts,
    at2.two_pointers_pct    AS away_2pt_pct,
    at2.three_pointers_pct  AS away_3pt_pct,
    at2.free_throws_pct     AS away_ft_pct,
    at2.assists             AS away_avg_ast,
    at2.turnovers           AS away_avg_tov,
    at2.offensive_rebounds  AS away_avg_oreb,
    at2.defensive_rebounds  AS away_avg_dreb,
    at2.pir                 AS away_avg_pir

FROM fact_games fg
LEFT JOIN fact_team_stats ht
    ON fg.home_team_code = ht.team_code
    AND fg.season_code   = ht.season_code
LEFT JOIN fact_team_stats at2
    ON fg.away_team_code = at2.team_code
    AND fg.season_code   = at2.season_code
WHERE
    fg.home_score IS NOT NULL
    AND fg.away_score IS NOT NULL
    AND ht.points_scored IS NOT NULL
    AND at2.points_scored IS NOT NULL
ORDER BY fg.season_year, fg.round_number
"""

df = pd.read_sql(query, conn)
conn.close()

print(f"Ucitano {len(df)} utakmica sa kompletnim podacima")
print(f"Sezone: {sorted(df['season_code'].unique())}")
print(f"Null vrednosti:\n{df.isnull().sum()[df.isnull().sum() > 0]}")

# =============================================================
# TRANSFORM — priprema featera
# =============================================================
print("\n" + "="*60)
print("TRANSFORM — priprema featera")
print("="*60)

# Phase encoding
phase_map = {'RS': 0, 'PO': 1, 'FF': 2}
df['phase_encoded'] = df['phase'].map(phase_map).fillna(0)

# Razlika izmedju timova — kljucni feature
df['pts_diff_avg']  = df['home_avg_pts']  - df['away_avg_pts']
df['pir_diff_avg']  = df['home_avg_pir']  - df['away_avg_pir']
df['ast_diff_avg']  = df['home_avg_ast']  - df['away_avg_ast']
df['tov_diff_avg']  = df['home_avg_tov']  - df['away_avg_tov']
df['oreb_diff_avg'] = df['home_avg_oreb'] - df['away_avg_oreb']

# Features lista
FEATURES = [
    'home_avg_pts', 'home_2pt_pct', 'home_3pt_pct', 'home_ft_pct',
    'home_avg_ast', 'home_avg_tov', 'home_avg_oreb', 'home_avg_dreb',
    'home_avg_pir',
    'away_avg_pts', 'away_2pt_pct', 'away_3pt_pct', 'away_ft_pct',
    'away_avg_ast', 'away_avg_tov', 'away_avg_oreb', 'away_avg_dreb',
    'away_avg_pir',
    'pts_diff_avg', 'pir_diff_avg', 'ast_diff_avg',
    'tov_diff_avg', 'oreb_diff_avg',
    'phase_encoded', 'season_year'
]

TARGET_HOME = 'home_score'
TARGET_AWAY = 'away_score'

df_clean = df[FEATURES + [TARGET_HOME, TARGET_AWAY]].dropna()
print(f"Cist dataset: {len(df_clean)} utakmica")
print(f"Features: {len(FEATURES)}")

# =============================================================
# TRAIN/TEST SPLIT — hronoloski
# =============================================================
print("\n" + "="*60)
print("TRAIN/TEST SPLIT — hronoloski (ne random!)")
print("="*60)

# Za vremenske serije uvek koristimo hronoloski split
# Train: 2015-2022, Test: 2023-2024
train = df_clean[df['season_year'] <= 2022]
test  = df_clean[df['season_year'] >  2022]

X_train = train[FEATURES]
X_test  = test[FEATURES]
y_train_home = train[TARGET_HOME]
y_test_home  = test[TARGET_HOME]
y_train_away = train[TARGET_AWAY]
y_test_away  = test[TARGET_AWAY]

print(f"Train: {len(train)} utakmica (2015-2022)")
print(f"Test:  {len(test)} utakmica (2023-2024)")

# Skaliranje
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled  = scaler.transform(X_test)

# =============================================================
# MODELI — treniraj i uporedi
# =============================================================
print("\n" + "="*60)
print("TRENIRANJE MODELA")
print("="*60)

models = {
    'Linear Regression': LinearRegression(),
    'Ridge Regression':  Ridge(alpha=1.0),
    'Random Forest':     RandomForestRegressor(
                            n_estimators=100,
                            max_depth=6,
                            random_state=42),
    'Gradient Boosting': GradientBoostingRegressor(
                            n_estimators=100,
                            max_depth=4,
                            learning_rate=0.1,
                            random_state=42),
}

results = []

for name, model in models.items():
    print(f"\nTreniram: {name}")

    # Home score
    if 'Forest' in name or 'Boosting' in name:
        model.fit(X_train, y_train_home)
        pred_home = model.predict(X_test)
    else:
        model.fit(X_train_scaled, y_train_home)
        pred_home = model.predict(X_test_scaled)

    mae_home  = mean_absolute_error(y_test_home, pred_home)
    rmse_home = np.sqrt(mean_squared_error(y_test_home, pred_home))
    r2_home   = r2_score(y_test_home, pred_home)

    results.append({
        'Model':  name,
        'Target': 'Home Score',
        'MAE':    round(mae_home,  2),
        'RMSE':   round(rmse_home, 2),
        'R2':     round(r2_home,   3),
    })

    print(f"  Home Score → MAE: {mae_home:.2f} | RMSE: {rmse_home:.2f} | R2: {r2_home:.3f}")

# =============================================================
# REZULTATI
# =============================================================
print("\n" + "="*60)
print("REZULTATI — poredjenje modela")
print("="*60)

results_df = pd.DataFrame(results)
print(results_df.to_string(index=False))

best = results_df.loc[results_df['MAE'].idxmin()]
print(f"\nNajbolji model: {best['Model']}")
print(f"  MAE:  {best['MAE']} poena  (prosecna greska)")
print(f"  RMSE: {best['RMSE']} poena")
print(f"  R2:   {best['R2']}  (1.0 = savrseno, 0 = loše)")

# =============================================================
# FEATURE IMPORTANCE — Random Forest
# =============================================================
print("\n" + "="*60)
print("FEATURE IMPORTANCE — Random Forest")
print("="*60)

rf = RandomForestRegressor(n_estimators=100, max_depth=6, random_state=42)
rf.fit(X_train, y_train_home)

importance_df = pd.DataFrame({
    'Feature':    FEATURES,
    'Importance': rf.feature_importances_
}).sort_values('Importance', ascending=False).head(10)

print(importance_df.to_string(index=False))

# =============================================================
# PRIMER PREDIKCIJE
# =============================================================
print("\n" + "="*60)
print("PRIMER — predikcija za konkretnu utakmicu")
print("="*60)

# Uzmemo prvu utakmicu iz test seta
sample = X_test.iloc[0:1]
actual_home = y_test_home.iloc[0]
actual_away = y_test_away.iloc[0]

pred = rf.predict(sample)[0]

print(f"Stvarni rezultat:    {int(actual_home)} : {int(actual_away)}")
print(f"Predvidjeni home:    {pred:.1f}")
print(f"Greska:              {abs(pred - actual_home):.1f} poena")

print("\n" + "="*60)
print("Model kompletiran!")
print("="*60)
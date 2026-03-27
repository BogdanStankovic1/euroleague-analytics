# Euroleague Analytics Project

End-to-end Data Analytics projekat baziran na 10 sezona Euroleague košarke (2015–2024).

## Tech Stack
- **Python 3.13** — ETL pipeline, data cleaning, ML model
- **MySQL 8.0** — Star Schema Data Warehouse
- **PowerBI** — Interaktivni report (4 stranice)
- **scikit-learn** — Regression model za predvidjanje poena

## Arhitektura — Star Schema
```
fact_games (2,897)          ── dim_season (10)
fact_player_stats (1,989)   ── dim_team (36)
fact_team_stats (180)       ── dim_player (752)
                            ── dim_calendar (~3,000)
```

## Struktura projekta
```
├── data/
│   └── raw/              # Sirovi CSV fajlovi (nisu na GitHubu)
├── scripts/
│   ├── 00_explore_api.py # Fetch podataka sa Euroleague API-ja
│   ├── 02_etl_pipeline.py# ETL za games i dimenzije
│   ├── 06_reload_stats.py# Per-season player/team stats
│   └── 07_prediction_model.py # ML regression model
├── SQL/
│   ├── create_tables.sql
│   └── 02_add_fk_games.sql
├── powerbi/
│   └── euroleague_report.pbix
└── requirements.txt
```

## Pokretanje

### 1. Instaliraj dependencies
```bash
pip install -r requirements.txt
```

### 2. Podesi MySQL konekciju
Kreiraj `scripts/utils.py` sa tvojim kredencijalima:
```python
DB_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": "tvoja_lozinka",
    "database": "euroleague_db"
}
```

### 3. Pokreni ETL
```bash
cd scripts
python 00_explore_api.py   # Fetch sirovih podataka
python 02_etl_pipeline.py  # Punjenje baze
python 06_reload_stats.py  # Per-season statistike
```

### 4. SQL setup
Izvrsiti u MySQL Workbench:
```sql
source SQL/create_tables.sql
source SQL/02_add_fk_games.sql
```

## Kljucni insights

- Prosecni skor porastao sa **77 (2015)** na **84 poena (2024)**
- Real Madrid dominira sa **187 pobeda** kroz 10 sezona
- Home win rate: **62.4%** — COVID sezona pala na 55%
- ML model MAE: **7.78 poena** (Linear Regression pobedio Random Forest)

## ML Model

Regression model predvidja `home_score` na osnovu 25 featera.

| Model | MAE | R² |
|---|---|---|
| Linear Regression | 7.78 | 0.159 |
| Ridge Regression | 7.78 | 0.160 |
| Random Forest | 7.79 | 0.155 |
| Gradient Boosting | 7.85 | 0.138 |

## PowerBI Report

Report sadrzi 4 stranice:
1. **Overview** — KPI kartice, scoring trend, top timovi
2. **Team Performance** — Scatter plot, bookmarks, Top N parametar
3. **Player Performance** — PIR analiza (u razvoju)
4. **Head-to-Head** — USERELATIONSHIP DAX (u razvoju)
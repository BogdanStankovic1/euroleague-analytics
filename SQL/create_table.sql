CREATE DATABASE IF NOT EXISTS euroleague_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;
 
USE euroleague_db;

DROP TABLE IF EXISTS dim_season;
CREATE TABLE dim_season (
    season_id       INT PRIMARY KEY AUTO_INCREMENT,
    season_year     INT          NOT NULL,   -- 2022, 2023, 2024
    season_code     VARCHAR(10)  NOT NULL,   -- E2022, E2023, E2024
    season_name     VARCHAR(50),             -- EuroLeague 2022-23
    season_alias    VARCHAR(20),             -- 2022-23
    start_date      DATE,
    UNIQUE KEY uq_season_code (season_code)
);

DROP TABLE IF EXISTS dim_team;
CREATE TABLE dim_team (
    team_id         INT PRIMARY KEY AUTO_INCREMENT,
    team_code       VARCHAR(10)  NOT NULL,   -- PAN, MAD, CSK...
    team_name       VARCHAR(100) NOT NULL,   -- Panathinaikos Athens
    team_tv_code    VARCHAR(20),             -- PAO, RMB...
    image_url       VARCHAR(500),
    UNIQUE KEY uq_team_code (team_code)
);

DROP TABLE IF EXISTS dim_player;
CREATE TABLE dim_player (
    player_id       INT PRIMARY KEY AUTO_INCREMENT,
    player_code     VARCHAR(20)  NOT NULL,   -- 008850, LUO...
    player_name     VARCHAR(100) NOT NULL,   -- LAZIC, BRANKO
    player_age      INT,
    image_url       VARCHAR(500),
    UNIQUE KEY uq_player_code (player_code)
);

DROP TABLE IF EXISTS dim_calendar;
CREATE TABLE dim_calendar (
    date_id         INT PRIMARY KEY AUTO_INCREMENT,
    full_date       DATE         NOT NULL,
    year            INT          NOT NULL,
    month           INT          NOT NULL,   -- 1-12
    month_name      VARCHAR(15),             -- October, November...
    day             INT          NOT NULL,
    day_of_week     INT,                     -- 1=Monday, 7=Sunday
    day_name        VARCHAR(15),             -- Monday, Tuesday...
    week_of_year    INT,
    quarter         INT,                     -- 1-4
    is_weekend      BOOLEAN,
    UNIQUE KEY uq_full_date (full_date)
);

DROP TABLE IF EXISTS fact_games;
CREATE TABLE fact_games (
    game_id             INT PRIMARY KEY AUTO_INCREMENT,
 
    -- Strani ključevi (FK) — veze sa dimenzijama
    season_id           INT          NOT NULL,
    date_id             INT,
 
    -- Identifikatori utakmice
    gamecode            INT          NOT NULL,   -- originalni kod iz API-ja
    season_year         INT          NOT NULL,   -- 2022, 2023, 2024
    season_code         VARCHAR(10)  NOT NULL,   -- E2022, E2023, E2024
    phase               VARCHAR(10),             -- RS, PO, FF
    round_number        INT,
    round_name          VARCHAR(30),
 
    -- Timovi (čuvamo kodove direktno — lakši ETL)
    home_team_code      VARCHAR(10)  NOT NULL,   -- local = domaćin
    away_team_code      VARCHAR(10)  NOT NULL,   -- road = gost
    home_team_name      VARCHAR(100),
    away_team_name      VARCHAR(100),
 
    -- Rezultat
    home_score          INT,
    away_score          INT,
    home_win            BOOLEAN,                 -- izračunato: home > away
    score_diff          INT,                     -- |home - away|
 
    -- Datum i vreme
    game_date           DATE,
    game_datetime_utc   DATETIME,
 
    -- Foreign key constraints
    CONSTRAINT fk_games_season
        FOREIGN KEY (season_id) REFERENCES dim_season(season_id),
    CONSTRAINT fk_games_date
        FOREIGN KEY (date_id) REFERENCES dim_calendar(date_id),
 
    -- Unique: jedna utakmica u jednoj sezoni
    UNIQUE KEY uq_game (season_year, gamecode)
);

DROP TABLE IF EXISTS fact_player_stats;
CREATE TABLE fact_player_stats (
    stat_id                     INT PRIMARY KEY AUTO_INCREMENT,
 
    -- Strani ključevi
    player_id                   INT          NOT NULL,
    season_id                   INT          NOT NULL,
    team_id                     INT,
 
    -- Originalni kodovi (za lakši ETL)
    player_code                 VARCHAR(20)  NOT NULL,
    team_code                   VARCHAR(10),
    season_code                 VARCHAR(10),
 
    -- Opšte
    games_played                INT,
    games_started               INT,
    minutes_played              DECIMAL(6,3),  -- prosek po utakmici
 
    -- Šutiranje
    points_scored               DECIMAL(5,2),
    two_pointers_made           DECIMAL(5,2),
    two_pointers_attempted      DECIMAL(5,2),
    two_pointers_pct            DECIMAL(5,3),  -- 0.576 umesto "57.6%"
    three_pointers_made         DECIMAL(5,2),
    three_pointers_attempted    DECIMAL(5,2),
    three_pointers_pct          DECIMAL(5,3),
    free_throws_made            DECIMAL(5,2),
    free_throws_attempted       DECIMAL(5,2),
    free_throws_pct             DECIMAL(5,3),
 
    -- Skokovi
    offensive_rebounds          DECIMAL(5,2),
    defensive_rebounds          DECIMAL(5,2),
    total_rebounds              DECIMAL(5,2),
 
    -- Ostale statistike
    assists                     DECIMAL(5,2),
    steals                      DECIMAL(5,2),
    turnovers                   DECIMAL(5,2),
    blocks                      DECIMAL(5,2),
    blocks_against              DECIMAL(5,2),
    fouls_committed             DECIMAL(5,2),
    fouls_drawn                 DECIMAL(5,2),
 
    -- Napredne metrike
    pir                         DECIMAL(6,2),  -- Performance Index Rating
    -- Ove računamo u ETL-u:
    true_shooting_pct           DECIMAL(5,3),  -- TS%
    assist_turnover_ratio       DECIMAL(5,3),  -- AST/TO
    rebound_rate                DECIMAL(5,3),  -- REB/min
 
    -- Foreign key constraints
    CONSTRAINT fk_pstats_player
        FOREIGN KEY (player_id) REFERENCES dim_player(player_id),
    CONSTRAINT fk_pstats_season
        FOREIGN KEY (season_id) REFERENCES dim_season(season_id),
 
    UNIQUE KEY uq_player_season (player_code, season_code)
);

DROP TABLE IF EXISTS fact_team_stats;
CREATE TABLE fact_team_stats (
    stat_id                     INT PRIMARY KEY AUTO_INCREMENT,
 
    -- Strani ključevi
    team_id                     INT          NOT NULL,
    season_id                   INT          NOT NULL,
 
    -- Originalni kodovi
    team_code                   VARCHAR(10)  NOT NULL,
    season_code                 VARCHAR(10),
 
    -- Opšte
    games_played                INT,
    minutes_played              DECIMAL(6,3),
 
    -- Šutiranje (prosek po utakmici)
    points_scored               DECIMAL(5,2),
    two_pointers_made           DECIMAL(5,2),
    two_pointers_attempted      DECIMAL(5,2),
    two_pointers_pct            DECIMAL(5,3),
    three_pointers_made         DECIMAL(5,2),
    three_pointers_attempted    DECIMAL(5,2),
    three_pointers_pct          DECIMAL(5,3),
    free_throws_made            DECIMAL(5,2),
    free_throws_attempted       DECIMAL(5,2),
    free_throws_pct             DECIMAL(5,3),
 
    -- Skokovi
    offensive_rebounds          DECIMAL(5,2),
    defensive_rebounds          DECIMAL(5,2),
    total_rebounds              DECIMAL(5,2),
 
    -- Ostalo
    assists                     DECIMAL(5,2),
    steals                      DECIMAL(5,2),
    turnovers                   DECIMAL(5,2),
    blocks                      DECIMAL(5,2),
    blocks_against              DECIMAL(5,2),
    fouls_committed             DECIMAL(5,2),
    fouls_drawn                 DECIMAL(5,2),
    pir                         DECIMAL(6,2),
 
    -- Foreign key constraints
    CONSTRAINT fk_tstats_team
        FOREIGN KEY (team_id) REFERENCES dim_team(team_id),
    CONSTRAINT fk_tstats_season
        FOREIGN KEY (season_id) REFERENCES dim_season(season_id),
 
    UNIQUE KEY uq_team_season (team_code, season_code)
);

SHOW TABLES;
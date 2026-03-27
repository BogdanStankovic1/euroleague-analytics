-- Dodaje home_team_id i away_team_id u fact_games
-- Pokreni jednom u MySQL Workbench

ALTER TABLE fact_games
    ADD COLUMN home_team_id INT,
    ADD COLUMN away_team_id INT,
    ADD CONSTRAINT fk_games_home
        FOREIGN KEY (home_team_id) REFERENCES dim_team(team_id),
    ADD CONSTRAINT fk_games_away
        FOREIGN KEY (away_team_id) REFERENCES dim_team(team_id);

UPDATE fact_games fg
JOIN dim_team dt ON fg.home_team_code = dt.team_code
SET fg.home_team_id = dt.team_id;

UPDATE fact_games fg
JOIN dim_team dt ON fg.away_team_code = dt.team_code
SET fg.away_team_id = dt.team_id;

-- Verifikacija
SELECT
    COUNT(*)                              AS ukupno_utakmica,
    SUM(home_team_id IS NULL)             AS bez_home_id,
    SUM(away_team_id IS NULL)             AS bez_away_id
FROM fact_games;

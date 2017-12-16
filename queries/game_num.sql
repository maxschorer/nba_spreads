SELECT
  s.season,
  game_date,
  team,
  RANK() OVER (PARTITION BY s.season, team ORDER BY game_date) AS game_num
FROM
  (SELECT
    game_date,
    away_team AS team
  FROM odds

  UNION
  SELECT
    game_date,
    home_team AS team
  FROM odds
  ) games
    INNER JOIN nba_season_dates s ON games.game_date >= s.start_date
    AND games.game_date <= s.end_date
SELECT
  AVG((o.vegas_spread - o.actual_spread)^2) AS mse
FROM odds o
  INNER JOIN nba_season_dates s ON o.game_date >= s.start_date
    AND o.game_date <= s.end_date
  INNER JOIN nba_team_pivot piv_away ON o.away_team = piv_away.oddsshark_code
  INNER JOIN nba_team_pivot piv_home ON o.home_team = piv_home.oddsshark_code
  INNER JOIN nba_macro_metrics_off macro_off_away ON piv_away.nba_team_code = macro_off_away.team_code
    AND o.game_date = macro_off_away.game_date
  INNER JOIN nba_macro_metrics_off macro_off_home ON piv_home.nba_team_code = macro_off_home.team_code
    AND o.game_date = macro_off_home.game_date
WHERE macro_off_away.game_num >= 20
  AND macro_off_home.game_num >= 20
  AND season = 2017
GROUP BY 1
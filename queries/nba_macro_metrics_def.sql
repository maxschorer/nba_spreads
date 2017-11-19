SELECT
  gl.season_year,
  piv1.nba_team_code AS team_code,
  piv1.nba_team_id AS team_id,
  games.game_date,
  gl.game_id,
  RANK() OVER (PARTITION BY season_year, team_abbreviation ORDER BY gl.game_date ) AS game_num,
  AVG(fgm) OVER (PARTITION BY season_year, team_abbreviation ORDER BY gl.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_off_fgm,
  AVG(fga) OVER (PARTITION BY season_year, team_abbreviation ORDER BY gl.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_off_fga,
  AVG(fg3m) OVER (PARTITION BY season_year, team_abbreviation ORDER BY gl.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_off_fg3m,
  AVG(fg3a) OVER (PARTITION BY season_year, team_abbreviation ORDER BY gl.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_off_fg3a,
  AVG(ast) OVER (PARTITION BY season_year, team_abbreviation ORDER BY gl.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_off_ast,
  AVG(blk) OVER (PARTITION BY season_year, team_abbreviation ORDER BY gl.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_off_blk,
  AVG(dreb) OVER (PARTITION BY season_year, team_abbreviation ORDER BY gl.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_off_dreb,
  AVG(oreb) OVER (PARTITION BY season_year, team_abbreviation ORDER BY gl.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_off_oreb,
  AVG(fta) OVER (PARTITION BY season_year, team_abbreviation ORDER BY gl.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_off_fta,
  AVG(ftm) OVER (PARTITION BY season_year, team_abbreviation ORDER BY gl.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_off_ftm,
  AVG(pts) OVER (PARTITION BY season_year, team_abbreviation ORDER BY gl.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_off_pts,
  AVG(reb) OVER (PARTITION BY season_year, team_abbreviation ORDER BY gl.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_off_reb,
  AVG(stl) OVER (PARTITION BY season_year, team_abbreviation ORDER BY gl.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_off_stl,
  AVG(tov) OVER (PARTITION BY season_year, team_abbreviation ORDER BY gl.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_off_tov
FROM
  (SELECT
    game_date,
    away_team AS team,
    home_team AS opp
  FROM odds

  UNION
  SELECT
    game_date,
    home_team AS team,
    away_team AS opp
  FROM odds
  ) games
  INNER JOIN nba_season_dates s ON games.game_date BETWEEN s.start_date AND s.end_date
  INNER JOIN nba_team_pivot piv1 ON games.team = piv1.oddsshark_code
  INNER JOIN nba_team_pivot piv2 ON games.opp = piv2.oddsshark_code
  INNER JOIN nba_team_game_logs gl ON piv2.nba_team_code = gl.team_abbreviation
    AND games.game_date = gl.game_date
ORDER BY games.game_date
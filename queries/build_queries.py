
MACRO_METRICS = ['avg_fgm','avg_fga','avg_fg3m','avg_fg3a','avg_ast','avg_blk','avg_dreb','avg_oreb','avg_fta','avg_ftm','avg_pts','avg_reb','avg_stl','avg_tov']
SHOT_TYPES = ['0-2 Feet - Very Tight', '2-4 Feet - Tight', '4-6 Feet - Open', '6+ Feet - Wide Open',
'0 Dribbles', '1 Dribble', '2 Dribbles', '3-6 Dribbles', '7+ Dribbles',
'24-22', '22-18 Very Early', '18-15 Early', '15-7 Average', '7-4 Late', '4-0 Very Late',
'Catch and Shoot', 'Less then 10 ft', 'Pullups', 'Totals',
'Touch < 2 Seconds', 'Touch 2-6 Seconds', 'Touch 6+ Seconds']
MICRO_METRICS = ['fga_frequency', 'fgm', 'fga', 'fg_pct', 'efg_pct', 'fg2a_frequency', 'fg2m', 'fg2a', 'fg2_pct', 'fg3a_frequency', 'fg3m', 'fg3a', 'fg3_pct']
TRAIN_SEASONS = [2014, 2015, 2016]
TEST_SEASON = [2017]

MICRO_METRICS_BY_GAMES = '''
SELECT
  game_date,
  season,
  nba_team_code,
  {}
FROM nba_teamdashptshots micro_stats
  INNER JOIN nba_team_pivot piv ON micro_stats.team_id = piv.nba_team_id
  INNER JOIN nba_season_dates s ON micro_stats.game_date BETWEEN s.start_date AND s.end_date
WHERE type NOT IN ('Unknown Touch Seconds', 'Not Captured', 'ShotClock Off')
  AND category NOT IN ('close_def_dist_range10ft_plus')
GROUP BY nba_team_code, game_date, season
'''

MICRO_METRICS_BY_GAME_CLAUSE = 'SUM(CASE WHEN type = \'{}\' THEN {} ELSE 0 END) AS {}_{}'

MICRO_METRICS_OFF = '''
SELECT
  game_date,
  nba_team_code,
  season,
  {}
FROM nba_micro_metrics_by_game
'''

MICRO_METRICS_OFF_CLAUSE = 'AVG({}_{}) OVER (PARTITION BY  nba_team_code, season ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_{}_{}'


MICRO_METRICS_DEF = '''
SELECT
  games.game_date,
  micro.nba_team_code,
  micro.season,
  {}
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
  INNER JOIN nba_team_pivot piv1 ON games.team = piv1.oddsshark_code
  INNER JOIN nba_team_pivot piv2 ON games.opp = piv2.oddsshark_code
  INNER JOIN nba_micro_metrics_by_game micro ON piv2.nba_team_code = micro.nba_team_code
    AND games.game_date = micro.game_date
'''

MICRO_METRICS_DEF_CLAUSE = 'AVG({}_{}) OVER (PARTITION BY  piv1.nba_team_code, micro.season ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_{}_{}'

MODEL_QUERY = '''
SELECT
  o.actual_spread,
  {}
FROM odds o
  INNER JOIN nba_season_dates s ON o.game_date BETWEEN s.start_date AND s.end_date
    AND s.season IN ({})
  INNER JOIN nba_team_pivot piv_away ON o.away_team = piv_away.oddsshark_code
  INNER JOIN nba_team_pivot piv_home ON o.home_team = piv_home.oddsshark_code
  INNER JOIN nba_macro_metrics_off macro_off_away ON piv_away.nba_team_code = macro_off_away.team_code
    AND o.game_date = macro_off_away.game_date
  INNER JOIN nba_macro_metrics_off macro_off_home ON piv_home.nba_team_code = macro_off_home.team_code
    AND o.game_date = macro_off_home.game_date
  INNER JOIN nba_macro_metrics_def macro_def_away ON piv_away.nba_team_code = macro_def_away.team_code
    AND o.game_date = macro_def_away.game_date
  INNER JOIN nba_macro_metrics_def macro_def_home ON piv_home.nba_team_code = macro_def_home.team_code
    AND o.game_date = macro_def_home.game_date
  INNER JOIN nba_micro_metrics_off micro_off_away ON piv_away.nba_team_code = micro_off_away.nba_team_code
    AND o.game_date = micro_off_away.game_date
  INNER JOIN nba_micro_metrics_off micro_off_home ON piv_home.nba_team_code = micro_off_home.nba_team_code
    AND o.game_date = micro_off_home.game_date
  INNER JOIN nba_micro_metrics_def micro_def_away ON piv_away.nba_team_code = micro_def_away.nba_team_code
    AND o.game_date = micro_def_away.game_date
  INNER JOIN nba_micro_metrics_def micro_def_home ON piv_home.nba_team_code = micro_def_home.nba_team_code
    AND o.game_date = micro_def_home.game_date

WHERE macro_off_away.game_num >= 20
  AND macro_off_home.game_num >= 20
'''

def main():
  # nba_micro_metrics_by_game
  select_clause = []
  for shot_type in SHOT_TYPES:
    shot_type_clean = shot_type.replace(' ','_').replace('+','plus').replace('-','_').replace('<','less').lower()
    for metric in MICRO_METRICS:
      col = MICRO_METRICS_BY_GAME_CLAUSE.format(shot_type, metric, metric, shot_type_clean)
      select_clause.append(col)

  micro_metrics_by_game_query = MICRO_METRICS_BY_GAMES.format(','.join(select_clause))
  print micro_metrics_by_game_query

  # nba_micro_metrics_off
  select_clause = []
  for shot_type in SHOT_TYPES:
    shot_type_clean = shot_type.replace(' ','_').replace('+','plus').replace('-','_').replace('<','less').lower()
    for metric in MICRO_METRICS:
      col = MICRO_METRICS_OFF_CLAUSE.format(metric, shot_type_clean, metric, shot_type_clean)
      select_clause.append(col)
  micro_metrics_off_query = MICRO_METRICS_OFF.format(',\n'.join(select_clause))
  print micro_metrics_off_query

  # nba_micro_metrics_def
  select_clause = []
  for shot_type in SHOT_TYPES:
    shot_type_clean = shot_type.replace(' ','_').replace('+','plus').replace('-','_').replace('<','less').lower()
    for metric in MICRO_METRICS:
      col = MICRO_METRICS_DEF_CLAUSE.format(metric, shot_type_clean, metric, shot_type_clean)
      select_clause.append(col)
  micro_metrics_def_query = MICRO_METRICS_DEF.format(',\n'.join(select_clause))
  print micro_metrics_def_query

  # training data
  select_clause = []
  for home_status in ['away', 'home']:
    for side_status in ['off', 'def']:
      for macro_stat in MACRO_METRICS:
        select_clause.append('macro_{}_{}.{} AS {}_{}_{}'.format(side_status, home_status, macro_stat, side_status, home_status, macro_stat))

      for shot_type in SHOT_TYPES:
        shot_type_clean = shot_type.replace(' ','_').replace('+','plus').replace('-','_').replace('<','less').lower()
        for stat in MICRO_METRICS:
          micro_stat = 'avg_{}_{}'.format(stat, shot_type_clean)
          column = 'micro_{}_{}.{} AS {}_{}_{}'.format(side_status, home_status, micro_stat, side_status, home_status, micro_stat)
          select_clause.append(column)

  model_query = MODEL_QUERY.format(',\n'.join(select_clause), '2014,2015,2016')
  print model_query
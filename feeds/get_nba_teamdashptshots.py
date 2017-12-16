# get_nba_teamdashptshots.py

import json
import pandas as pd
import requests
from utils import *

HOME_DIR = 'NBA_SPREAD_PATH'
NBA_HEADERS = 'secure/nba_headers.json'
NBA_TEAMS = 'secure/nba_teams.json'
SEASONS = [2014, 2016, 2017]

URL = 'https://stats.nba.com/stats/teamdashptshots?DateFrom={}&DateTo={}&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=PerGame&Period=0&PlusMinus=N&Rank=N&Season={}&SeasonSegment=&SeasonType=Regular+Season&TeamID={}&VsConference=&VsDivision='

TABLES = ['general_shooting', 'shot_clock_shooting', 'dribble_shooting',
'closest_defender_shooting', 'closest_defender10ft_plus_shooting', 'touch_time_shooting']

COLUMNS =  ['game_date', 'team_id', 'category', 'type',
  'FGA_FREQUENCY', 'FGM', 'FGA', 'FG_PCT', 'EFG_PCT',
  'FG2A_FREQUENCY', 'FG2M', 'FG2A', 'FG2_PCT',
  'FG3A_FREQUENCY', 'FG3M', 'FG3A', 'FG3_PCT']

def get_game_dates(connection, team_id, season):
  QUERY = '''
  SELECT
    game_date
  FROM
  (
  SELECT
    game_date,
    season,
    nba_team_id
  FROM odds o
    INNER JOIN nba_season_dates s ON game_date BETWEEN s.start_date AND s.end_date
    INNER JOIN nba_team_pivot piv ON o.away_team = piv.oddsshark_code

  UNION

  SELECT
    game_date,
    season,
    nba_team_id
  FROM odds o
    INNER JOIN nba_season_dates s ON game_date BETWEEN s.start_date AND s.end_date
    INNER JOIN nba_team_pivot piv ON o.home_team = piv.oddsshark_code
  ) t
  WHERE season = {}
    AND nba_team_id = {}
  ORDER BY game_date
  '''

  query = QUERY.format(season, team_id)
  raw_df = query_to_df(connection, query)
  return [str(d) for d in raw_df['game_date'].tolist()]


def main():
  home_dir = get_env(HOME_DIR)
  headers = json_to_dict(home_dir + NBA_HEADERS)
  teams = json_to_dict(home_dir + NBA_TEAMS)
  results_list = []
  connection = get_connection()
  season_games = set()
  #for team_id, team in teams.iteritems():
  for season in SEASONS:
    for team_id, team in teams.iteritems():
      game_dates = get_game_dates(connection, team_id, season)
      for game_date in game_dates:
        if (game_date, team_id) in season_games: continue
        game_date_list = game_date.split('-')
        game_date_str = '%2F'.join(game_date_list[1:] + game_date_list[:1])
        season_code = str(season-1) + '-' + str(season)[2:]
        url = URL.format(game_date_str, game_date_str, season_code, team_id)
        result_sets = json.loads(requests.get(url, headers=headers).text)['resultSets']
        for result_set in result_sets:
          name = convert(result_set['name'])
          feature = result_set['headers'][4].lower()
          if name.find('10') > 0:
            feature += '10ft_plus'
          # update columns
          for row in result_set['rowSet']:
            results_list.append([game_date, team_id, feature] + row[4:])
        season_games.add((game_date, team_id))
        print 'Completed {}-{}'.format(game_date, team)

  results_df = pd.DataFrame(results_list, columns=COLUMNS)
  results_df['FG3_PCT'].fillna(SENTINEL, inplace=True)
  results_df['FG2_PCT'].fillna(SENTINEL, inplace=True)

  insert_df_into_table(connection, results_df, 'nba_teamdashptshots')

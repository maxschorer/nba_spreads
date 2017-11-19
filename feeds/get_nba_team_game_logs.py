# get_nba_team_game_logs.py

from bs4 import BeautifulSoup
import json
import pandas as pd
import requests
from utils import *

HOME_DIR = 'NBA_SPREAD_PATH'
NBA_HEADERS = 'secure/nba_headers.json'
NBA_TEAMS = 'secure/nba_teams.json'
SEASONS = [2014, 2015, 2016, 2017]

URL = 'https://stats.nba.com/stats/teamgamelogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season={}&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&TeamID={}&VsConference=&VsDivision='

def main():
  home_dir = get_env(HOME_DIR)
  headers = json_to_dict(home_dir + NBA_HEADERS)
  teams = json_to_dict(home_dir + NBA_TEAMS)
  columns = None
  rows = []
  for team_id, team in teams.iteritems():
    for season in SEASONS:
      season_code = str(season-1) + '-' + str(season)[2:]
      url = URL.format(season_code, team_id)
      results_json = json.loads(requests.get(url, headers=headers).text)
      if columns is None: columns = results_json['resultSets'][0]['headers']
      rows += results_json['resultSets'][0]['rowSet']

  game_logs_df = pd.DataFrame(rows, columns=columns)
  connection = get_connection()
  insert_df_into_table(connection, game_logs_df, 'nba_team_game_logs')

#scrape_spreads.py
from bs4 import BeautifulSoup
import pandas as pd
import requests
from utils import *

ODDSSHARK_HEADERS = 'secure/oddsshark_headers.json'
ODDSSHARK_TEAMS = 'secure/oddsshark_teams.json'
HOME_DIR = 'NBA_SPREAD_PATH'


def generate_distinct_combos(teams):
  teams = list(teams)
  teams.sort()

  combos = []
  for i in range(len(teams) - 1):
    for j in range(i+1, len(teams)):
      combos.append((teams[i], teams[j]))

  return combos


def scrape_historical_odds(combo, headers):
  URL = 'http://www.oddsshark.com/stats/dbresults/basketball/nba'
  DATA = 'selected-values=H2H%7C{}%7C{}%7C30%7C0%7CREG%7CANY%7CANY&honeypot-time-stamp=1510893922&team-search=Boston&opponent-search=Denver&number-games=on&location=on&numeric=1&numeric=30'

  data = DATA.format(combo[0], combo[1])
  headers['Content-Length'] = str(len(data))
  html = requests.post(URL, data=data, headers=headers).text
  soup = BeautifulSoup(html)
  table = soup.find('table')
  rows = table.findAll('tr')[1:]
  return rows


def load_odds_into_db(connection, all_odds):
  odds_df = pd.DataFrame(all_odds,columns=['game_date', 'away_team', 'away_team_points', 'home_team', 'home_team_points', 'game_result', 'vegas_spread', 'spread_result', 'ou', 'ou_result'])
  DROP_COLS = ['game_result', 'ou', 'ou_result', 'spread_result']
  INT_COLS = ['away_team_points', 'home_team_points']
  FLOAT_COLS = ['vegas_spread']

  for col in DROP_COLS:
    del odds_df[col]

  for col in INT_COLS:
    odds_df[col] = odds_df[col].astype(int)

  for col in FLOAT_COLS:
    odds_df[col] = odds_df[col].astype(float)

  odds_df['actual_spread'] = odds_df['away_team_points'] - odds_df['home_team_points']


  insert_df_into_table(connection, odds_df, 'odds')


def main():
  home_dir = get_env(HOME_DIR)
  headers = json_to_dict(home_dir + ODDSSHARK_HEADERS)
  teams = json_to_dict(home_dir + ODDSSHARK_TEAMS)
  # read in teams
  combos = generate_distinct_combos([t['value'] for t in TEAMS])
  all_odds = []
  for combo in combos:
    all_odds += scrape_historical_odds(combo, HEADERS)
  connection = get_connection()
  load_odds_into_db(connection, all_odds)
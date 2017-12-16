import json
import logging
import os
import pandas as pd
import psycopg2
import re

# CONSTANTS
SENTINEL = -1.0

# Functions
def json_to_dict(json_file_path):
  json_data = open(json_file_path)
  dict_data = json.load(json_data)
  json_data.close()
  return dict_data


def get_connection(file_path=None):
  if file_path is None:
    file_path = get_env('PG_CREDS')

  pg_creds = json_to_dict(file_path)
  return psycopg2.connect(host=pg_creds['host'], database=pg_creds['database'],
      user=pg_creds['user'], password=pg_creds['password'])


def query_to_df(connection, query):
  cur = connection.cursor()
  cur.execute(query)

  cols = [desc[0] for desc in cur.description]
  output = cur.fetchall()
  if not output:
    df = pd.DataFrame(columns = cols)
  else:
    df = pd.DataFrame(output, columns = cols)

  return df


def create_tuple_string(df):
  NUMBER_TYPES = ['int64', 'float64']
  BOOL_TYPES = ['bool']
  STRING_TYPES = ['object']

  columns = df.columns
  output = []

  for column in columns:
    data_type = str(df[column].dtype)
    if data_type in NUMBER_TYPES:
      output.append('%s')
    elif data_type in BOOL_TYPES:
      output.append('%s')
    elif data_type in STRING_TYPES:
      output.append('\'%s\'')

  return ', '.join(output)


def insert_df_into_table(connection, df, table):
  BASE_QUERY = '''
  INSERT INTO %(table)s
  (%(columns)s)
  VALUES (%(tuple_string)s)
  '''

  cursor = connection.cursor()

  columns = ', '.join([str(c) for c in df.columns])
  tuple_string = create_tuple_string(df)

  query_template = BASE_QUERY % {'table': table, 'columns': columns, 'tuple_string': tuple_string}

  for idx, row in df.iterrows():
    row_tuple = tuple(row)
    query = query_template.replace('\n', ' ') % row_tuple
    try:
      cursor.execute(query)
      connection.commit()
    except Exception, e:
      print e
      connection.rollback()
      print e
      logging.error('Error entering row')
      logging.error(e)


def get_env(var):
  return os.environ.get(var)


def convert(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def file_to_string(file_path):
  f = open(file_path)
  lines = f.readlines()
  f.close()
  return ''.join(lines)
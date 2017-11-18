CREATE TABLE odds (
  game_date DATE,
  away_team VARCHAR(8),
  away_team_points SMALLINT,
  home_team VARCHAR(8),
  home_team_points SMALLINT,
  vegas_spread NUMERIC,
  actual_spread SMALLINT
)
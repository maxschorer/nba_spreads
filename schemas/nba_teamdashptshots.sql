CREATE TABLE nba_teamdashptshots (
  game_date DATE,
  team_id BIGINT,
  category VARCHAR(32),
  type VARCHAR(32),
  fga_frequency NUMERIC,
  fgm SMALLINT,
  fga SMALLINT,
  fg_pct NUMERIC,
  efg_pct NUMERIC,
  fg2a_frequency NUMERIC,
  fg2m SMALLINT,
  fg2a SMALLINT,
  fg2_pct NUMERIC,
  fg3a_frequency NUMERIC,
  fg3m SMALLINT,
  fg3a SMALLINT,
  fg3_pct NUMERIC
)
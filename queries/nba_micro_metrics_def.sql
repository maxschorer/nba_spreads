SELECT
  games.game_date,
  piv1.nba_team_code AS team_code,
  AVG(fgm_touch_6plus_seconds) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_touch_6plus_seconds,
  AVG(fga_touch_6plus_seconds) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_touch_6plus_seconds,
  AVG(fg2m_touch_6plus_seconds) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_touch_6plus_seconds,
  AVG(fg2a_touch_6plus_seconds) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_touch_6plus_seconds,
  AVG(fg3m_touch_6plus_seconds) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_touch_6plus_seconds,
  AVG(fg3a_touch_6plus_seconds) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_touch_6plus_seconds,
  AVG(fgm_totals) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_totals,
  AVG(fga_totals) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_totals,
  AVG(fg2m_totals) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_totals,
  AVG(fg2a_totals) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_totals,
  AVG(fg3m_totals) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_totals,
  AVG(fg3a_totals) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_totals,
  AVG(fgm_7_4_late) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_7_4_late,
  AVG(fga_7_4_late) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_7_4_late,
  AVG(fg2m_7_4_late) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_7_4_late,
  AVG(fg2a_7_4_late) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_7_4_late,
  AVG(fg3m_7_4_late) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_7_4_late,
  AVG(fg3a_7_4_late) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_7_4_late,
  AVG(fgm_7plus_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_7plus_dribbles,
  AVG(fga_7plus_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_7plus_dribbles,
  AVG(fg2m_7plus_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_7plus_dribbles,
  AVG(fg2a_7plus_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_7plus_dribbles,
  AVG(fg3m_7plus_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_7plus_dribbles,
  AVG(fg3a_7plus_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_7plus_dribbles,
  AVG(fgm_1_dribble) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_1_dribble,
  AVG(fga_1_dribble) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_1_dribble,
  AVG(fg2m_1_dribble) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_1_dribble,
  AVG(fg2a_1_dribble) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_1_dribble,
  AVG(fg3m_1_dribble) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_1_dribble,
  AVG(fg3a_1_dribble) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_1_dribble,
  AVG(fgm_catch_and_shoot) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_catch_and_shoot,
  AVG(fga_catch_and_shoot) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_catch_and_shoot,
  AVG(fg2m_catch_and_shoot) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_catch_and_shoot,
  AVG(fg2a_catch_and_shoot) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_catch_and_shoot,
  AVG(fg3m_catch_and_shoot) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_catch_and_shoot,
  AVG(fg3a_catch_and_shoot) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_catch_and_shoot,
  AVG(fgm_22_18_very_early) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_22_18_very_early,
  AVG(fga_22_18_very_early) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_22_18_very_early,
  AVG(fg2m_22_18_very_early) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_22_18_very_early,
  AVG(fg2a_22_18_very_early) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_22_18_very_early,
  AVG(fg3m_22_18_very_early) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_22_18_very_early,
  AVG(fg3a_22_18_very_early) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_22_18_very_early,
  AVG(fgm_4_0_very_late) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_4_0_very_late,
  AVG(fga_4_0_very_late) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_4_0_very_late,
  AVG(fg2m_4_0_very_late) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_4_0_very_late,
  AVG(fg2a_4_0_very_late) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_4_0_very_late,
  AVG(fg3m_4_0_very_late) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_4_0_very_late,
  AVG(fg3a_4_0_very_late) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_4_0_very_late,
  AVG(fgm_4_6_feet___open) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_4_6_feet___open,
  AVG(fga_4_6_feet___open) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_4_6_feet___open,
  AVG(fg2m_4_6_feet___open) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_4_6_feet___open,
  AVG(fg2a_4_6_feet___open) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_4_6_feet___open,
  AVG(fg3m_4_6_feet___open) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_4_6_feet___open,
  AVG(fg3a_4_6_feet___open) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_4_6_feet___open,
  AVG(fgm_3_6_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_3_6_dribbles,
  AVG(fga_3_6_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_3_6_dribbles,
  AVG(fg2m_3_6_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_3_6_dribbles,
  AVG(fg2a_3_6_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_3_6_dribbles,
  AVG(fg3m_3_6_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_3_6_dribbles,
  AVG(fg3a_3_6_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_3_6_dribbles,
  AVG(fgm_15_7_average) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_15_7_average,
  AVG(fga_15_7_average) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_15_7_average,
  AVG(fg2m_15_7_average) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_15_7_average,
  AVG(fg2a_15_7_average) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_15_7_average,
  AVG(fg3m_15_7_average) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_15_7_average,
  AVG(fg3a_15_7_average) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_15_7_average,
  AVG(fgm_2_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_2_dribbles,
  AVG(fga_2_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_2_dribbles,
  AVG(fg2m_2_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_2_dribbles,
  AVG(fg2a_2_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_2_dribbles,
  AVG(fg3m_2_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_2_dribbles,
  AVG(fg3a_2_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_2_dribbles,
  AVG(fgm_18_15_early) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_18_15_early,
  AVG(fga_18_15_early) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_18_15_early,
  AVG(fg2m_18_15_early) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_18_15_early,
  AVG(fg2a_18_15_early) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_18_15_early,
  AVG(fg3m_18_15_early) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_18_15_early,
  AVG(fg3a_18_15_early) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_18_15_early,
  AVG(fgm_touch_less_2_seconds) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_touch_less_2_seconds,
  AVG(fga_touch_less_2_seconds) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_touch_less_2_seconds,
  AVG(fg2m_touch_less_2_seconds) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_touch_less_2_seconds,
  AVG(fg2a_touch_less_2_seconds) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_touch_less_2_seconds,
  AVG(fg3m_touch_less_2_seconds) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_touch_less_2_seconds,
  AVG(fg3a_touch_less_2_seconds) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_touch_less_2_seconds,
  AVG(fgm_2_4_feet___tight) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_2_4_feet___tight,
  AVG(fga_2_4_feet___tight) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_2_4_feet___tight,
  AVG(fg2m_2_4_feet___tight) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_2_4_feet___tight,
  AVG(fg2a_2_4_feet___tight) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_2_4_feet___tight,
  AVG(fg3m_2_4_feet___tight) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_2_4_feet___tight,
  AVG(fg3a_2_4_feet___tight) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_2_4_feet___tight,
  AVG(fgm_6plus_feet___wide_open) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_6plus_feet___wide_open,
  AVG(fga_6plus_feet___wide_open) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_6plus_feet___wide_open,
  AVG(fg2m_6plus_feet___wide_open) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_6plus_feet___wide_open,
  AVG(fg2a_6plus_feet___wide_open) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_6plus_feet___wide_open,
  AVG(fg3m_6plus_feet___wide_open) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_6plus_feet___wide_open,
  AVG(fg3a_6plus_feet___wide_open) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_6plus_feet___wide_open,
  AVG(fgm_0_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_0_dribbles,
  AVG(fga_0_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_0_dribbles,
  AVG(fg2m_0_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_0_dribbles,
  AVG(fg2a_0_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_0_dribbles,
  AVG(fg3m_0_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_0_dribbles,
  AVG(fg3a_0_dribbles) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_0_dribbles,
  AVG(fgm_touch_2_6_seconds) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_touch_2_6_seconds,
  AVG(fga_touch_2_6_seconds) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_touch_2_6_seconds,
  AVG(fg2m_touch_2_6_seconds) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_touch_2_6_seconds,
  AVG(fg2a_touch_2_6_seconds) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_touch_2_6_seconds,
  AVG(fg3m_touch_2_6_seconds) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_touch_2_6_seconds,
  AVG(fg3a_touch_2_6_seconds) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_touch_2_6_seconds,
  AVG(fgm_less_then_10_ft) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_less_then_10_ft,
  AVG(fga_less_then_10_ft) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_less_then_10_ft,
  AVG(fg2m_less_then_10_ft) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_less_then_10_ft,
  AVG(fg2a_less_then_10_ft) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_less_then_10_ft,
  AVG(fg3m_less_then_10_ft) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_less_then_10_ft,
  AVG(fg3a_less_then_10_ft) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_less_then_10_ft,
  AVG(fgm_0_2_feet___very_tight) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_0_2_feet___very_tight,
  AVG(fga_0_2_feet___very_tight) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_0_2_feet___very_tight,
  AVG(fg2m_0_2_feet___very_tight) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_0_2_feet___very_tight,
  AVG(fg2a_0_2_feet___very_tight) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_0_2_feet___very_tight,
  AVG(fg3m_0_2_feet___very_tight) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_0_2_feet___very_tight,
  AVG(fg3a_0_2_feet___very_tight) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_0_2_feet___very_tight,
  AVG(fgm_pullups) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_pullups,
  AVG(fga_pullups) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_pullups,
  AVG(fg2m_pullups) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_pullups,
  AVG(fg2a_pullups) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_pullups,
  AVG(fg3m_pullups) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_pullups,
  AVG(fg3a_pullups) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_pullups,
  AVG(fgm_24_22) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fgm_24_22,
  AVG(fga_24_22) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fga_24_22,
  AVG(fg2m_24_22) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2m_24_22,
  AVG(fg2a_24_22) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg2a_24_22,
  AVG(fg3m_24_22) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3m_24_22,
  AVG(fg3a_24_22) OVER (PARTITION BY  piv1.nba_team_code ORDER BY games.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_fg3a_24_22
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
  INNER JOIN nba_micro_metrics_by_game micro ON piv2.nba_team_code = micro.nba_team_code
    AND games.game_date = micro.game_date
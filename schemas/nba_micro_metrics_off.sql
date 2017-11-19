CREATE TABLE nba_micro_metrics_off (
  game_date DATE,
  nba_team_code VARCHAR(8),
  avg_fgm_touch_6plus_seconds SMALLINT,
  avg_fga_touch_6plus_seconds SMALLINT,
  avg_fg2m_touch_6plus_seconds SMALLINT,
  avg_fg2a_touch_6plus_seconds SMALLINT,
  avg_fg3m_touch_6plus_seconds SMALLINT,
  avg_fg3a_touch_6plus_seconds SMALLINT,
  avg_fgm_totals SMALLINT,
  avg_fga_totals SMALLINT,
  avg_fg2m_totals SMALLINT,
  avg_fg2a_totals SMALLINT,
  avg_fg3m_totals SMALLINT,
  avg_fg3a_totals SMALLINT,
  avg_fgm_7_4_late SMALLINT,
  avg_fga_7_4_late SMALLINT,
  avg_fg2m_7_4_late SMALLINT,
  avg_fg2a_7_4_late SMALLINT,
  avg_fg3m_7_4_late SMALLINT,
  avg_fg3a_7_4_late SMALLINT,
  avg_fgm_7plus_dribbles SMALLINT,
  avg_fga_7plus_dribbles SMALLINT,
  avg_fg2m_7plus_dribbles SMALLINT,
  avg_fg2a_7plus_dribbles SMALLINT,
  avg_fg3m_7plus_dribbles SMALLINT,
  avg_fg3a_7plus_dribbles SMALLINT,
  avg_fgm_1_dribble SMALLINT,
  avg_fga_1_dribble SMALLINT,
  avg_fg2m_1_dribble SMALLINT,
  avg_fg2a_1_dribble SMALLINT,
  avg_fg3m_1_dribble SMALLINT,
  avg_fg3a_1_dribble SMALLINT,
  avg_fgm_catch_and_shoot SMALLINT,
  avg_fga_catch_and_shoot SMALLINT,
  avg_fg2m_catch_and_shoot SMALLINT,
  avg_fg2a_catch_and_shoot SMALLINT,
  avg_fg3m_catch_and_shoot SMALLINT,
  avg_fg3a_catch_and_shoot SMALLINT,
  avg_fgm_22_18_very_early SMALLINT,
  avg_fga_22_18_very_early SMALLINT,
  avg_fg2m_22_18_very_early SMALLINT,
  avg_fg2a_22_18_very_early SMALLINT,
  avg_fg3m_22_18_very_early SMALLINT,
  avg_fg3a_22_18_very_early SMALLINT,
  avg_fgm_4_0_very_late SMALLINT,
  avg_fga_4_0_very_late SMALLINT,
  avg_fg2m_4_0_very_late SMALLINT,
  avg_fg2a_4_0_very_late SMALLINT,
  avg_fg3m_4_0_very_late SMALLINT,
  avg_fg3a_4_0_very_late SMALLINT,
  avg_fgm_4_6_feet___open SMALLINT,
  avg_fga_4_6_feet___open SMALLINT,
  avg_fg2m_4_6_feet___open SMALLINT,
  avg_fg2a_4_6_feet___open SMALLINT,
  avg_fg3m_4_6_feet___open SMALLINT,
  avg_fg3a_4_6_feet___open SMALLINT,
  avg_fgm_3_6_dribbles SMALLINT,
  avg_fga_3_6_dribbles SMALLINT,
  avg_fg2m_3_6_dribbles SMALLINT,
  avg_fg2a_3_6_dribbles SMALLINT,
  avg_fg3m_3_6_dribbles SMALLINT,
  avg_fg3a_3_6_dribbles SMALLINT,
  avg_fgm_15_7_average SMALLINT,
  avg_fga_15_7_average SMALLINT,
  avg_fg2m_15_7_average SMALLINT,
  avg_fg2a_15_7_average SMALLINT,
  avg_fg3m_15_7_average SMALLINT,
  avg_fg3a_15_7_average SMALLINT,
  avg_fgm_2_dribbles SMALLINT,
  avg_fga_2_dribbles SMALLINT,
  avg_fg2m_2_dribbles SMALLINT,
  avg_fg2a_2_dribbles SMALLINT,
  avg_fg3m_2_dribbles SMALLINT,
  avg_fg3a_2_dribbles SMALLINT,
  avg_fgm_18_15_early SMALLINT,
  avg_fga_18_15_early SMALLINT,
  avg_fg2m_18_15_early SMALLINT,
  avg_fg2a_18_15_early SMALLINT,
  avg_fg3m_18_15_early SMALLINT,
  avg_fg3a_18_15_early SMALLINT,
  avg_fgm_touch_less_2_seconds SMALLINT,
  avg_fga_touch_less_2_seconds SMALLINT,
  avg_fg2m_touch_less_2_seconds SMALLINT,
  avg_fg2a_touch_less_2_seconds SMALLINT,
  avg_fg3m_touch_less_2_seconds SMALLINT,
  avg_fg3a_touch_less_2_seconds SMALLINT,
  avg_fgm_2_4_feet___tight SMALLINT,
  avg_fga_2_4_feet___tight SMALLINT,
  avg_fg2m_2_4_feet___tight SMALLINT,
  avg_fg2a_2_4_feet___tight SMALLINT,
  avg_fg3m_2_4_feet___tight SMALLINT,
  avg_fg3a_2_4_feet___tight SMALLINT,
  avg_fgm_6plus_feet___wide_open SMALLINT,
  avg_fga_6plus_feet___wide_open SMALLINT,
  avg_fg2m_6plus_feet___wide_open SMALLINT,
  avg_fg2a_6plus_feet___wide_open SMALLINT,
  avg_fg3m_6plus_feet___wide_open SMALLINT,
  avg_fg3a_6plus_feet___wide_open SMALLINT,
  avg_fgm_0_dribbles SMALLINT,
  avg_fga_0_dribbles SMALLINT,
  avg_fg2m_0_dribbles SMALLINT,
  avg_fg2a_0_dribbles SMALLINT,
  avg_fg3m_0_dribbles SMALLINT,
  avg_fg3a_0_dribbles SMALLINT,
  avg_fgm_touch_2_6_seconds SMALLINT,
  avg_fga_touch_2_6_seconds SMALLINT,
  avg_fg2m_touch_2_6_seconds SMALLINT,
  avg_fg2a_touch_2_6_seconds SMALLINT,
  avg_fg3m_touch_2_6_seconds SMALLINT,
  avg_fg3a_touch_2_6_seconds SMALLINT,
  avg_fgm_less_then_10_ft SMALLINT,
  avg_fga_less_then_10_ft SMALLINT,
  avg_fg2m_less_then_10_ft SMALLINT,
  avg_fg2a_less_then_10_ft SMALLINT,
  avg_fg3m_less_then_10_ft SMALLINT,
  avg_fg3a_less_then_10_ft SMALLINT,
  avg_fgm_0_2_feet___very_tight SMALLINT,
  avg_fga_0_2_feet___very_tight SMALLINT,
  avg_fg2m_0_2_feet___very_tight SMALLINT,
  avg_fg2a_0_2_feet___very_tight SMALLINT,
  avg_fg3m_0_2_feet___very_tight SMALLINT,
  avg_fg3a_0_2_feet___very_tight SMALLINT,
  avg_fgm_pullups SMALLINT,
  avg_fga_pullups SMALLINT,
  avg_fg2m_pullups SMALLINT,
  avg_fg2a_pullups SMALLINT,
  avg_fg3m_pullups SMALLINT,
  avg_fg3a_pullups SMALLINT,
  avg_fgm_24_22 SMALLINT,
  avg_fga_24_22 SMALLINT,
  avg_fg2m_24_22 SMALLINT,
  avg_fg2a_24_22 SMALLINT,
  avg_fg3m_24_22 SMALLINT,
  avg_fg3a_24_22 SMALLINT
)
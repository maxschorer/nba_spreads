SELECT
  micro_stats.game_date,
  nba_team_code,
  s.season,
  SUM(CASE WHEN type = '0-2 Feet - Very Tight' THEN fga_frequency ELSE 0 END) AS fga_frequency_0_2_feet___very_tight,
SUM(CASE WHEN type = '0-2 Feet - Very Tight' THEN fgm ELSE 0 END) AS fgm_0_2_feet___very_tight,
SUM(CASE WHEN type = '0-2 Feet - Very Tight' THEN fga ELSE 0 END) AS fga_0_2_feet___very_tight,
SUM(CASE WHEN type = '0-2 Feet - Very Tight' THEN fg_pct ELSE 0 END) AS fg_pct_0_2_feet___very_tight,
SUM(CASE WHEN type = '0-2 Feet - Very Tight' THEN efg_pct ELSE 0 END) AS efg_pct_0_2_feet___very_tight,
SUM(CASE WHEN type = '0-2 Feet - Very Tight' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_0_2_feet___very_tight,
SUM(CASE WHEN type = '0-2 Feet - Very Tight' THEN fg2m ELSE 0 END) AS fg2m_0_2_feet___very_tight,
SUM(CASE WHEN type = '0-2 Feet - Very Tight' THEN fg2a ELSE 0 END) AS fg2a_0_2_feet___very_tight,
SUM(CASE WHEN type = '0-2 Feet - Very Tight' THEN fg2_pct ELSE 0 END) AS fg2_pct_0_2_feet___very_tight,
SUM(CASE WHEN type = '0-2 Feet - Very Tight' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_0_2_feet___very_tight,
SUM(CASE WHEN type = '0-2 Feet - Very Tight' THEN fg3m ELSE 0 END) AS fg3m_0_2_feet___very_tight,
SUM(CASE WHEN type = '0-2 Feet - Very Tight' THEN fg3a ELSE 0 END) AS fg3a_0_2_feet___very_tight,
SUM(CASE WHEN type = '0-2 Feet - Very Tight' THEN fg3_pct ELSE 0 END) AS fg3_pct_0_2_feet___very_tight,
SUM(CASE WHEN type = '2-4 Feet - Tight' THEN fga_frequency ELSE 0 END) AS fga_frequency_2_4_feet___tight,
SUM(CASE WHEN type = '2-4 Feet - Tight' THEN fgm ELSE 0 END) AS fgm_2_4_feet___tight,
SUM(CASE WHEN type = '2-4 Feet - Tight' THEN fga ELSE 0 END) AS fga_2_4_feet___tight,
SUM(CASE WHEN type = '2-4 Feet - Tight' THEN fg_pct ELSE 0 END) AS fg_pct_2_4_feet___tight,
SUM(CASE WHEN type = '2-4 Feet - Tight' THEN efg_pct ELSE 0 END) AS efg_pct_2_4_feet___tight,
SUM(CASE WHEN type = '2-4 Feet - Tight' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_2_4_feet___tight,
SUM(CASE WHEN type = '2-4 Feet - Tight' THEN fg2m ELSE 0 END) AS fg2m_2_4_feet___tight,
SUM(CASE WHEN type = '2-4 Feet - Tight' THEN fg2a ELSE 0 END) AS fg2a_2_4_feet___tight,
SUM(CASE WHEN type = '2-4 Feet - Tight' THEN fg2_pct ELSE 0 END) AS fg2_pct_2_4_feet___tight,
SUM(CASE WHEN type = '2-4 Feet - Tight' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_2_4_feet___tight,
SUM(CASE WHEN type = '2-4 Feet - Tight' THEN fg3m ELSE 0 END) AS fg3m_2_4_feet___tight,
SUM(CASE WHEN type = '2-4 Feet - Tight' THEN fg3a ELSE 0 END) AS fg3a_2_4_feet___tight,
SUM(CASE WHEN type = '2-4 Feet - Tight' THEN fg3_pct ELSE 0 END) AS fg3_pct_2_4_feet___tight,
SUM(CASE WHEN type = '4-6 Feet - Open' THEN fga_frequency ELSE 0 END) AS fga_frequency_4_6_feet___open,
SUM(CASE WHEN type = '4-6 Feet - Open' THEN fgm ELSE 0 END) AS fgm_4_6_feet___open,
SUM(CASE WHEN type = '4-6 Feet - Open' THEN fga ELSE 0 END) AS fga_4_6_feet___open,
SUM(CASE WHEN type = '4-6 Feet - Open' THEN fg_pct ELSE 0 END) AS fg_pct_4_6_feet___open,
SUM(CASE WHEN type = '4-6 Feet - Open' THEN efg_pct ELSE 0 END) AS efg_pct_4_6_feet___open,
SUM(CASE WHEN type = '4-6 Feet - Open' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_4_6_feet___open,
SUM(CASE WHEN type = '4-6 Feet - Open' THEN fg2m ELSE 0 END) AS fg2m_4_6_feet___open,
SUM(CASE WHEN type = '4-6 Feet - Open' THEN fg2a ELSE 0 END) AS fg2a_4_6_feet___open,
SUM(CASE WHEN type = '4-6 Feet - Open' THEN fg2_pct ELSE 0 END) AS fg2_pct_4_6_feet___open,
SUM(CASE WHEN type = '4-6 Feet - Open' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_4_6_feet___open,
SUM(CASE WHEN type = '4-6 Feet - Open' THEN fg3m ELSE 0 END) AS fg3m_4_6_feet___open,
SUM(CASE WHEN type = '4-6 Feet - Open' THEN fg3a ELSE 0 END) AS fg3a_4_6_feet___open,
SUM(CASE WHEN type = '4-6 Feet - Open' THEN fg3_pct ELSE 0 END) AS fg3_pct_4_6_feet___open,
SUM(CASE WHEN type = '6+ Feet - Wide Open' THEN fga_frequency ELSE 0 END) AS fga_frequency_6plus_feet___wide_open,
SUM(CASE WHEN type = '6+ Feet - Wide Open' THEN fgm ELSE 0 END) AS fgm_6plus_feet___wide_open,
SUM(CASE WHEN type = '6+ Feet - Wide Open' THEN fga ELSE 0 END) AS fga_6plus_feet___wide_open,
SUM(CASE WHEN type = '6+ Feet - Wide Open' THEN fg_pct ELSE 0 END) AS fg_pct_6plus_feet___wide_open,
SUM(CASE WHEN type = '6+ Feet - Wide Open' THEN efg_pct ELSE 0 END) AS efg_pct_6plus_feet___wide_open,
SUM(CASE WHEN type = '6+ Feet - Wide Open' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_6plus_feet___wide_open,
SUM(CASE WHEN type = '6+ Feet - Wide Open' THEN fg2m ELSE 0 END) AS fg2m_6plus_feet___wide_open,
SUM(CASE WHEN type = '6+ Feet - Wide Open' THEN fg2a ELSE 0 END) AS fg2a_6plus_feet___wide_open,
SUM(CASE WHEN type = '6+ Feet - Wide Open' THEN fg2_pct ELSE 0 END) AS fg2_pct_6plus_feet___wide_open,
SUM(CASE WHEN type = '6+ Feet - Wide Open' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_6plus_feet___wide_open,
SUM(CASE WHEN type = '6+ Feet - Wide Open' THEN fg3m ELSE 0 END) AS fg3m_6plus_feet___wide_open,
SUM(CASE WHEN type = '6+ Feet - Wide Open' THEN fg3a ELSE 0 END) AS fg3a_6plus_feet___wide_open,
SUM(CASE WHEN type = '6+ Feet - Wide Open' THEN fg3_pct ELSE 0 END) AS fg3_pct_6plus_feet___wide_open,
SUM(CASE WHEN type = '0 Dribbles' THEN fga_frequency ELSE 0 END) AS fga_frequency_0_dribbles,
SUM(CASE WHEN type = '0 Dribbles' THEN fgm ELSE 0 END) AS fgm_0_dribbles,
SUM(CASE WHEN type = '0 Dribbles' THEN fga ELSE 0 END) AS fga_0_dribbles,
SUM(CASE WHEN type = '0 Dribbles' THEN fg_pct ELSE 0 END) AS fg_pct_0_dribbles,
SUM(CASE WHEN type = '0 Dribbles' THEN efg_pct ELSE 0 END) AS efg_pct_0_dribbles,
SUM(CASE WHEN type = '0 Dribbles' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_0_dribbles,
SUM(CASE WHEN type = '0 Dribbles' THEN fg2m ELSE 0 END) AS fg2m_0_dribbles,
SUM(CASE WHEN type = '0 Dribbles' THEN fg2a ELSE 0 END) AS fg2a_0_dribbles,
SUM(CASE WHEN type = '0 Dribbles' THEN fg2_pct ELSE 0 END) AS fg2_pct_0_dribbles,
SUM(CASE WHEN type = '0 Dribbles' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_0_dribbles,
SUM(CASE WHEN type = '0 Dribbles' THEN fg3m ELSE 0 END) AS fg3m_0_dribbles,
SUM(CASE WHEN type = '0 Dribbles' THEN fg3a ELSE 0 END) AS fg3a_0_dribbles,
SUM(CASE WHEN type = '0 Dribbles' THEN fg3_pct ELSE 0 END) AS fg3_pct_0_dribbles,
SUM(CASE WHEN type = '1 Dribble' THEN fga_frequency ELSE 0 END) AS fga_frequency_1_dribble,
SUM(CASE WHEN type = '1 Dribble' THEN fgm ELSE 0 END) AS fgm_1_dribble,
SUM(CASE WHEN type = '1 Dribble' THEN fga ELSE 0 END) AS fga_1_dribble,
SUM(CASE WHEN type = '1 Dribble' THEN fg_pct ELSE 0 END) AS fg_pct_1_dribble,
SUM(CASE WHEN type = '1 Dribble' THEN efg_pct ELSE 0 END) AS efg_pct_1_dribble,
SUM(CASE WHEN type = '1 Dribble' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_1_dribble,
SUM(CASE WHEN type = '1 Dribble' THEN fg2m ELSE 0 END) AS fg2m_1_dribble,
SUM(CASE WHEN type = '1 Dribble' THEN fg2a ELSE 0 END) AS fg2a_1_dribble,
SUM(CASE WHEN type = '1 Dribble' THEN fg2_pct ELSE 0 END) AS fg2_pct_1_dribble,
SUM(CASE WHEN type = '1 Dribble' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_1_dribble,
SUM(CASE WHEN type = '1 Dribble' THEN fg3m ELSE 0 END) AS fg3m_1_dribble,
SUM(CASE WHEN type = '1 Dribble' THEN fg3a ELSE 0 END) AS fg3a_1_dribble,
SUM(CASE WHEN type = '1 Dribble' THEN fg3_pct ELSE 0 END) AS fg3_pct_1_dribble,
SUM(CASE WHEN type = '2 Dribbles' THEN fga_frequency ELSE 0 END) AS fga_frequency_2_dribbles,
SUM(CASE WHEN type = '2 Dribbles' THEN fgm ELSE 0 END) AS fgm_2_dribbles,
SUM(CASE WHEN type = '2 Dribbles' THEN fga ELSE 0 END) AS fga_2_dribbles,
SUM(CASE WHEN type = '2 Dribbles' THEN fg_pct ELSE 0 END) AS fg_pct_2_dribbles,
SUM(CASE WHEN type = '2 Dribbles' THEN efg_pct ELSE 0 END) AS efg_pct_2_dribbles,
SUM(CASE WHEN type = '2 Dribbles' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_2_dribbles,
SUM(CASE WHEN type = '2 Dribbles' THEN fg2m ELSE 0 END) AS fg2m_2_dribbles,
SUM(CASE WHEN type = '2 Dribbles' THEN fg2a ELSE 0 END) AS fg2a_2_dribbles,
SUM(CASE WHEN type = '2 Dribbles' THEN fg2_pct ELSE 0 END) AS fg2_pct_2_dribbles,
SUM(CASE WHEN type = '2 Dribbles' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_2_dribbles,
SUM(CASE WHEN type = '2 Dribbles' THEN fg3m ELSE 0 END) AS fg3m_2_dribbles,
SUM(CASE WHEN type = '2 Dribbles' THEN fg3a ELSE 0 END) AS fg3a_2_dribbles,
SUM(CASE WHEN type = '2 Dribbles' THEN fg3_pct ELSE 0 END) AS fg3_pct_2_dribbles,
SUM(CASE WHEN type = '3-6 Dribbles' THEN fga_frequency ELSE 0 END) AS fga_frequency_3_6_dribbles,
SUM(CASE WHEN type = '3-6 Dribbles' THEN fgm ELSE 0 END) AS fgm_3_6_dribbles,
SUM(CASE WHEN type = '3-6 Dribbles' THEN fga ELSE 0 END) AS fga_3_6_dribbles,
SUM(CASE WHEN type = '3-6 Dribbles' THEN fg_pct ELSE 0 END) AS fg_pct_3_6_dribbles,
SUM(CASE WHEN type = '3-6 Dribbles' THEN efg_pct ELSE 0 END) AS efg_pct_3_6_dribbles,
SUM(CASE WHEN type = '3-6 Dribbles' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_3_6_dribbles,
SUM(CASE WHEN type = '3-6 Dribbles' THEN fg2m ELSE 0 END) AS fg2m_3_6_dribbles,
SUM(CASE WHEN type = '3-6 Dribbles' THEN fg2a ELSE 0 END) AS fg2a_3_6_dribbles,
SUM(CASE WHEN type = '3-6 Dribbles' THEN fg2_pct ELSE 0 END) AS fg2_pct_3_6_dribbles,
SUM(CASE WHEN type = '3-6 Dribbles' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_3_6_dribbles,
SUM(CASE WHEN type = '3-6 Dribbles' THEN fg3m ELSE 0 END) AS fg3m_3_6_dribbles,
SUM(CASE WHEN type = '3-6 Dribbles' THEN fg3a ELSE 0 END) AS fg3a_3_6_dribbles,
SUM(CASE WHEN type = '3-6 Dribbles' THEN fg3_pct ELSE 0 END) AS fg3_pct_3_6_dribbles,
SUM(CASE WHEN type = '7+ Dribbles' THEN fga_frequency ELSE 0 END) AS fga_frequency_7plus_dribbles,
SUM(CASE WHEN type = '7+ Dribbles' THEN fgm ELSE 0 END) AS fgm_7plus_dribbles,
SUM(CASE WHEN type = '7+ Dribbles' THEN fga ELSE 0 END) AS fga_7plus_dribbles,
SUM(CASE WHEN type = '7+ Dribbles' THEN fg_pct ELSE 0 END) AS fg_pct_7plus_dribbles,
SUM(CASE WHEN type = '7+ Dribbles' THEN efg_pct ELSE 0 END) AS efg_pct_7plus_dribbles,
SUM(CASE WHEN type = '7+ Dribbles' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_7plus_dribbles,
SUM(CASE WHEN type = '7+ Dribbles' THEN fg2m ELSE 0 END) AS fg2m_7plus_dribbles,
SUM(CASE WHEN type = '7+ Dribbles' THEN fg2a ELSE 0 END) AS fg2a_7plus_dribbles,
SUM(CASE WHEN type = '7+ Dribbles' THEN fg2_pct ELSE 0 END) AS fg2_pct_7plus_dribbles,
SUM(CASE WHEN type = '7+ Dribbles' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_7plus_dribbles,
SUM(CASE WHEN type = '7+ Dribbles' THEN fg3m ELSE 0 END) AS fg3m_7plus_dribbles,
SUM(CASE WHEN type = '7+ Dribbles' THEN fg3a ELSE 0 END) AS fg3a_7plus_dribbles,
SUM(CASE WHEN type = '7+ Dribbles' THEN fg3_pct ELSE 0 END) AS fg3_pct_7plus_dribbles,
SUM(CASE WHEN type = '24-22' THEN fga_frequency ELSE 0 END) AS fga_frequency_24_22,
SUM(CASE WHEN type = '24-22' THEN fgm ELSE 0 END) AS fgm_24_22,
SUM(CASE WHEN type = '24-22' THEN fga ELSE 0 END) AS fga_24_22,
SUM(CASE WHEN type = '24-22' THEN fg_pct ELSE 0 END) AS fg_pct_24_22,
SUM(CASE WHEN type = '24-22' THEN efg_pct ELSE 0 END) AS efg_pct_24_22,
SUM(CASE WHEN type = '24-22' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_24_22,
SUM(CASE WHEN type = '24-22' THEN fg2m ELSE 0 END) AS fg2m_24_22,
SUM(CASE WHEN type = '24-22' THEN fg2a ELSE 0 END) AS fg2a_24_22,
SUM(CASE WHEN type = '24-22' THEN fg2_pct ELSE 0 END) AS fg2_pct_24_22,
SUM(CASE WHEN type = '24-22' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_24_22,
SUM(CASE WHEN type = '24-22' THEN fg3m ELSE 0 END) AS fg3m_24_22,
SUM(CASE WHEN type = '24-22' THEN fg3a ELSE 0 END) AS fg3a_24_22,
SUM(CASE WHEN type = '24-22' THEN fg3_pct ELSE 0 END) AS fg3_pct_24_22,
SUM(CASE WHEN type = '22-18 Very Early' THEN fga_frequency ELSE 0 END) AS fga_frequency_22_18_very_early,
SUM(CASE WHEN type = '22-18 Very Early' THEN fgm ELSE 0 END) AS fgm_22_18_very_early,
SUM(CASE WHEN type = '22-18 Very Early' THEN fga ELSE 0 END) AS fga_22_18_very_early,
SUM(CASE WHEN type = '22-18 Very Early' THEN fg_pct ELSE 0 END) AS fg_pct_22_18_very_early,
SUM(CASE WHEN type = '22-18 Very Early' THEN efg_pct ELSE 0 END) AS efg_pct_22_18_very_early,
SUM(CASE WHEN type = '22-18 Very Early' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_22_18_very_early,
SUM(CASE WHEN type = '22-18 Very Early' THEN fg2m ELSE 0 END) AS fg2m_22_18_very_early,
SUM(CASE WHEN type = '22-18 Very Early' THEN fg2a ELSE 0 END) AS fg2a_22_18_very_early,
SUM(CASE WHEN type = '22-18 Very Early' THEN fg2_pct ELSE 0 END) AS fg2_pct_22_18_very_early,
SUM(CASE WHEN type = '22-18 Very Early' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_22_18_very_early,
SUM(CASE WHEN type = '22-18 Very Early' THEN fg3m ELSE 0 END) AS fg3m_22_18_very_early,
SUM(CASE WHEN type = '22-18 Very Early' THEN fg3a ELSE 0 END) AS fg3a_22_18_very_early,
SUM(CASE WHEN type = '22-18 Very Early' THEN fg3_pct ELSE 0 END) AS fg3_pct_22_18_very_early,
SUM(CASE WHEN type = '18-15 Early' THEN fga_frequency ELSE 0 END) AS fga_frequency_18_15_early,
SUM(CASE WHEN type = '18-15 Early' THEN fgm ELSE 0 END) AS fgm_18_15_early,
SUM(CASE WHEN type = '18-15 Early' THEN fga ELSE 0 END) AS fga_18_15_early,
SUM(CASE WHEN type = '18-15 Early' THEN fg_pct ELSE 0 END) AS fg_pct_18_15_early,
SUM(CASE WHEN type = '18-15 Early' THEN efg_pct ELSE 0 END) AS efg_pct_18_15_early,
SUM(CASE WHEN type = '18-15 Early' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_18_15_early,
SUM(CASE WHEN type = '18-15 Early' THEN fg2m ELSE 0 END) AS fg2m_18_15_early,
SUM(CASE WHEN type = '18-15 Early' THEN fg2a ELSE 0 END) AS fg2a_18_15_early,
SUM(CASE WHEN type = '18-15 Early' THEN fg2_pct ELSE 0 END) AS fg2_pct_18_15_early,
SUM(CASE WHEN type = '18-15 Early' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_18_15_early,
SUM(CASE WHEN type = '18-15 Early' THEN fg3m ELSE 0 END) AS fg3m_18_15_early,
SUM(CASE WHEN type = '18-15 Early' THEN fg3a ELSE 0 END) AS fg3a_18_15_early,
SUM(CASE WHEN type = '18-15 Early' THEN fg3_pct ELSE 0 END) AS fg3_pct_18_15_early,
SUM(CASE WHEN type = '15-7 Average' THEN fga_frequency ELSE 0 END) AS fga_frequency_15_7_average,
SUM(CASE WHEN type = '15-7 Average' THEN fgm ELSE 0 END) AS fgm_15_7_average,
SUM(CASE WHEN type = '15-7 Average' THEN fga ELSE 0 END) AS fga_15_7_average,
SUM(CASE WHEN type = '15-7 Average' THEN fg_pct ELSE 0 END) AS fg_pct_15_7_average,
SUM(CASE WHEN type = '15-7 Average' THEN efg_pct ELSE 0 END) AS efg_pct_15_7_average,
SUM(CASE WHEN type = '15-7 Average' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_15_7_average,
SUM(CASE WHEN type = '15-7 Average' THEN fg2m ELSE 0 END) AS fg2m_15_7_average,
SUM(CASE WHEN type = '15-7 Average' THEN fg2a ELSE 0 END) AS fg2a_15_7_average,
SUM(CASE WHEN type = '15-7 Average' THEN fg2_pct ELSE 0 END) AS fg2_pct_15_7_average,
SUM(CASE WHEN type = '15-7 Average' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_15_7_average,
SUM(CASE WHEN type = '15-7 Average' THEN fg3m ELSE 0 END) AS fg3m_15_7_average,
SUM(CASE WHEN type = '15-7 Average' THEN fg3a ELSE 0 END) AS fg3a_15_7_average,
SUM(CASE WHEN type = '15-7 Average' THEN fg3_pct ELSE 0 END) AS fg3_pct_15_7_average,
SUM(CASE WHEN type = '7-4 Late' THEN fga_frequency ELSE 0 END) AS fga_frequency_7_4_late,
SUM(CASE WHEN type = '7-4 Late' THEN fgm ELSE 0 END) AS fgm_7_4_late,
SUM(CASE WHEN type = '7-4 Late' THEN fga ELSE 0 END) AS fga_7_4_late,
SUM(CASE WHEN type = '7-4 Late' THEN fg_pct ELSE 0 END) AS fg_pct_7_4_late,
SUM(CASE WHEN type = '7-4 Late' THEN efg_pct ELSE 0 END) AS efg_pct_7_4_late,
SUM(CASE WHEN type = '7-4 Late' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_7_4_late,
SUM(CASE WHEN type = '7-4 Late' THEN fg2m ELSE 0 END) AS fg2m_7_4_late,
SUM(CASE WHEN type = '7-4 Late' THEN fg2a ELSE 0 END) AS fg2a_7_4_late,
SUM(CASE WHEN type = '7-4 Late' THEN fg2_pct ELSE 0 END) AS fg2_pct_7_4_late,
SUM(CASE WHEN type = '7-4 Late' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_7_4_late,
SUM(CASE WHEN type = '7-4 Late' THEN fg3m ELSE 0 END) AS fg3m_7_4_late,
SUM(CASE WHEN type = '7-4 Late' THEN fg3a ELSE 0 END) AS fg3a_7_4_late,
SUM(CASE WHEN type = '7-4 Late' THEN fg3_pct ELSE 0 END) AS fg3_pct_7_4_late,
SUM(CASE WHEN type = '4-0 Very Late' THEN fga_frequency ELSE 0 END) AS fga_frequency_4_0_very_late,
SUM(CASE WHEN type = '4-0 Very Late' THEN fgm ELSE 0 END) AS fgm_4_0_very_late,
SUM(CASE WHEN type = '4-0 Very Late' THEN fga ELSE 0 END) AS fga_4_0_very_late,
SUM(CASE WHEN type = '4-0 Very Late' THEN fg_pct ELSE 0 END) AS fg_pct_4_0_very_late,
SUM(CASE WHEN type = '4-0 Very Late' THEN efg_pct ELSE 0 END) AS efg_pct_4_0_very_late,
SUM(CASE WHEN type = '4-0 Very Late' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_4_0_very_late,
SUM(CASE WHEN type = '4-0 Very Late' THEN fg2m ELSE 0 END) AS fg2m_4_0_very_late,
SUM(CASE WHEN type = '4-0 Very Late' THEN fg2a ELSE 0 END) AS fg2a_4_0_very_late,
SUM(CASE WHEN type = '4-0 Very Late' THEN fg2_pct ELSE 0 END) AS fg2_pct_4_0_very_late,
SUM(CASE WHEN type = '4-0 Very Late' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_4_0_very_late,
SUM(CASE WHEN type = '4-0 Very Late' THEN fg3m ELSE 0 END) AS fg3m_4_0_very_late,
SUM(CASE WHEN type = '4-0 Very Late' THEN fg3a ELSE 0 END) AS fg3a_4_0_very_late,
SUM(CASE WHEN type = '4-0 Very Late' THEN fg3_pct ELSE 0 END) AS fg3_pct_4_0_very_late,
SUM(CASE WHEN type = 'Catch and Shoot' THEN fga_frequency ELSE 0 END) AS fga_frequency_catch_and_shoot,
SUM(CASE WHEN type = 'Catch and Shoot' THEN fgm ELSE 0 END) AS fgm_catch_and_shoot,
SUM(CASE WHEN type = 'Catch and Shoot' THEN fga ELSE 0 END) AS fga_catch_and_shoot,
SUM(CASE WHEN type = 'Catch and Shoot' THEN fg_pct ELSE 0 END) AS fg_pct_catch_and_shoot,
SUM(CASE WHEN type = 'Catch and Shoot' THEN efg_pct ELSE 0 END) AS efg_pct_catch_and_shoot,
SUM(CASE WHEN type = 'Catch and Shoot' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_catch_and_shoot,
SUM(CASE WHEN type = 'Catch and Shoot' THEN fg2m ELSE 0 END) AS fg2m_catch_and_shoot,
SUM(CASE WHEN type = 'Catch and Shoot' THEN fg2a ELSE 0 END) AS fg2a_catch_and_shoot,
SUM(CASE WHEN type = 'Catch and Shoot' THEN fg2_pct ELSE 0 END) AS fg2_pct_catch_and_shoot,
SUM(CASE WHEN type = 'Catch and Shoot' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_catch_and_shoot,
SUM(CASE WHEN type = 'Catch and Shoot' THEN fg3m ELSE 0 END) AS fg3m_catch_and_shoot,
SUM(CASE WHEN type = 'Catch and Shoot' THEN fg3a ELSE 0 END) AS fg3a_catch_and_shoot,
SUM(CASE WHEN type = 'Catch and Shoot' THEN fg3_pct ELSE 0 END) AS fg3_pct_catch_and_shoot,
SUM(CASE WHEN type = 'Less then 10 ft' THEN fga_frequency ELSE 0 END) AS fga_frequency_less_then_10_ft,
SUM(CASE WHEN type = 'Less then 10 ft' THEN fgm ELSE 0 END) AS fgm_less_then_10_ft,
SUM(CASE WHEN type = 'Less then 10 ft' THEN fga ELSE 0 END) AS fga_less_then_10_ft,
SUM(CASE WHEN type = 'Less then 10 ft' THEN fg_pct ELSE 0 END) AS fg_pct_less_then_10_ft,
SUM(CASE WHEN type = 'Less then 10 ft' THEN efg_pct ELSE 0 END) AS efg_pct_less_then_10_ft,
SUM(CASE WHEN type = 'Less then 10 ft' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_less_then_10_ft,
SUM(CASE WHEN type = 'Less then 10 ft' THEN fg2m ELSE 0 END) AS fg2m_less_then_10_ft,
SUM(CASE WHEN type = 'Less then 10 ft' THEN fg2a ELSE 0 END) AS fg2a_less_then_10_ft,
SUM(CASE WHEN type = 'Less then 10 ft' THEN fg2_pct ELSE 0 END) AS fg2_pct_less_then_10_ft,
SUM(CASE WHEN type = 'Less then 10 ft' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_less_then_10_ft,
SUM(CASE WHEN type = 'Less then 10 ft' THEN fg3m ELSE 0 END) AS fg3m_less_then_10_ft,
SUM(CASE WHEN type = 'Less then 10 ft' THEN fg3a ELSE 0 END) AS fg3a_less_then_10_ft,
SUM(CASE WHEN type = 'Less then 10 ft' THEN fg3_pct ELSE 0 END) AS fg3_pct_less_then_10_ft,
SUM(CASE WHEN type = 'Pullups' THEN fga_frequency ELSE 0 END) AS fga_frequency_pullups,
SUM(CASE WHEN type = 'Pullups' THEN fgm ELSE 0 END) AS fgm_pullups,
SUM(CASE WHEN type = 'Pullups' THEN fga ELSE 0 END) AS fga_pullups,
SUM(CASE WHEN type = 'Pullups' THEN fg_pct ELSE 0 END) AS fg_pct_pullups,
SUM(CASE WHEN type = 'Pullups' THEN efg_pct ELSE 0 END) AS efg_pct_pullups,
SUM(CASE WHEN type = 'Pullups' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_pullups,
SUM(CASE WHEN type = 'Pullups' THEN fg2m ELSE 0 END) AS fg2m_pullups,
SUM(CASE WHEN type = 'Pullups' THEN fg2a ELSE 0 END) AS fg2a_pullups,
SUM(CASE WHEN type = 'Pullups' THEN fg2_pct ELSE 0 END) AS fg2_pct_pullups,
SUM(CASE WHEN type = 'Pullups' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_pullups,
SUM(CASE WHEN type = 'Pullups' THEN fg3m ELSE 0 END) AS fg3m_pullups,
SUM(CASE WHEN type = 'Pullups' THEN fg3a ELSE 0 END) AS fg3a_pullups,
SUM(CASE WHEN type = 'Pullups' THEN fg3_pct ELSE 0 END) AS fg3_pct_pullups,
SUM(CASE WHEN type = 'Totals' THEN fga_frequency ELSE 0 END) AS fga_frequency_totals,
SUM(CASE WHEN type = 'Totals' THEN fgm ELSE 0 END) AS fgm_totals,
SUM(CASE WHEN type = 'Totals' THEN fga ELSE 0 END) AS fga_totals,
SUM(CASE WHEN type = 'Totals' THEN fg_pct ELSE 0 END) AS fg_pct_totals,
SUM(CASE WHEN type = 'Totals' THEN efg_pct ELSE 0 END) AS efg_pct_totals,
SUM(CASE WHEN type = 'Totals' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_totals,
SUM(CASE WHEN type = 'Totals' THEN fg2m ELSE 0 END) AS fg2m_totals,
SUM(CASE WHEN type = 'Totals' THEN fg2a ELSE 0 END) AS fg2a_totals,
SUM(CASE WHEN type = 'Totals' THEN fg2_pct ELSE 0 END) AS fg2_pct_totals,
SUM(CASE WHEN type = 'Totals' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_totals,
SUM(CASE WHEN type = 'Totals' THEN fg3m ELSE 0 END) AS fg3m_totals,
SUM(CASE WHEN type = 'Totals' THEN fg3a ELSE 0 END) AS fg3a_totals,
SUM(CASE WHEN type = 'Totals' THEN fg3_pct ELSE 0 END) AS fg3_pct_totals,
SUM(CASE WHEN type = 'Touch < 2 Seconds' THEN fga_frequency ELSE 0 END) AS fga_frequency_touch_less_2_seconds,
SUM(CASE WHEN type = 'Touch < 2 Seconds' THEN fgm ELSE 0 END) AS fgm_touch_less_2_seconds,
SUM(CASE WHEN type = 'Touch < 2 Seconds' THEN fga ELSE 0 END) AS fga_touch_less_2_seconds,
SUM(CASE WHEN type = 'Touch < 2 Seconds' THEN fg_pct ELSE 0 END) AS fg_pct_touch_less_2_seconds,
SUM(CASE WHEN type = 'Touch < 2 Seconds' THEN efg_pct ELSE 0 END) AS efg_pct_touch_less_2_seconds,
SUM(CASE WHEN type = 'Touch < 2 Seconds' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_touch_less_2_seconds,
SUM(CASE WHEN type = 'Touch < 2 Seconds' THEN fg2m ELSE 0 END) AS fg2m_touch_less_2_seconds,
SUM(CASE WHEN type = 'Touch < 2 Seconds' THEN fg2a ELSE 0 END) AS fg2a_touch_less_2_seconds,
SUM(CASE WHEN type = 'Touch < 2 Seconds' THEN fg2_pct ELSE 0 END) AS fg2_pct_touch_less_2_seconds,
SUM(CASE WHEN type = 'Touch < 2 Seconds' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_touch_less_2_seconds,
SUM(CASE WHEN type = 'Touch < 2 Seconds' THEN fg3m ELSE 0 END) AS fg3m_touch_less_2_seconds,
SUM(CASE WHEN type = 'Touch < 2 Seconds' THEN fg3a ELSE 0 END) AS fg3a_touch_less_2_seconds,
SUM(CASE WHEN type = 'Touch < 2 Seconds' THEN fg3_pct ELSE 0 END) AS fg3_pct_touch_less_2_seconds,
SUM(CASE WHEN type = 'Touch 2-6 Seconds' THEN fga_frequency ELSE 0 END) AS fga_frequency_touch_2_6_seconds,
SUM(CASE WHEN type = 'Touch 2-6 Seconds' THEN fgm ELSE 0 END) AS fgm_touch_2_6_seconds,
SUM(CASE WHEN type = 'Touch 2-6 Seconds' THEN fga ELSE 0 END) AS fga_touch_2_6_seconds,
SUM(CASE WHEN type = 'Touch 2-6 Seconds' THEN fg_pct ELSE 0 END) AS fg_pct_touch_2_6_seconds,
SUM(CASE WHEN type = 'Touch 2-6 Seconds' THEN efg_pct ELSE 0 END) AS efg_pct_touch_2_6_seconds,
SUM(CASE WHEN type = 'Touch 2-6 Seconds' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_touch_2_6_seconds,
SUM(CASE WHEN type = 'Touch 2-6 Seconds' THEN fg2m ELSE 0 END) AS fg2m_touch_2_6_seconds,
SUM(CASE WHEN type = 'Touch 2-6 Seconds' THEN fg2a ELSE 0 END) AS fg2a_touch_2_6_seconds,
SUM(CASE WHEN type = 'Touch 2-6 Seconds' THEN fg2_pct ELSE 0 END) AS fg2_pct_touch_2_6_seconds,
SUM(CASE WHEN type = 'Touch 2-6 Seconds' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_touch_2_6_seconds,
SUM(CASE WHEN type = 'Touch 2-6 Seconds' THEN fg3m ELSE 0 END) AS fg3m_touch_2_6_seconds,
SUM(CASE WHEN type = 'Touch 2-6 Seconds' THEN fg3a ELSE 0 END) AS fg3a_touch_2_6_seconds,
SUM(CASE WHEN type = 'Touch 2-6 Seconds' THEN fg3_pct ELSE 0 END) AS fg3_pct_touch_2_6_seconds,
SUM(CASE WHEN type = 'Touch 6+ Seconds' THEN fga_frequency ELSE 0 END) AS fga_frequency_touch_6plus_seconds,
SUM(CASE WHEN type = 'Touch 6+ Seconds' THEN fgm ELSE 0 END) AS fgm_touch_6plus_seconds,
SUM(CASE WHEN type = 'Touch 6+ Seconds' THEN fga ELSE 0 END) AS fga_touch_6plus_seconds,
SUM(CASE WHEN type = 'Touch 6+ Seconds' THEN fg_pct ELSE 0 END) AS fg_pct_touch_6plus_seconds,
SUM(CASE WHEN type = 'Touch 6+ Seconds' THEN efg_pct ELSE 0 END) AS efg_pct_touch_6plus_seconds,
SUM(CASE WHEN type = 'Touch 6+ Seconds' THEN fg2a_frequency ELSE 0 END) AS fg2a_frequency_touch_6plus_seconds,
SUM(CASE WHEN type = 'Touch 6+ Seconds' THEN fg2m ELSE 0 END) AS fg2m_touch_6plus_seconds,
SUM(CASE WHEN type = 'Touch 6+ Seconds' THEN fg2a ELSE 0 END) AS fg2a_touch_6plus_seconds,
SUM(CASE WHEN type = 'Touch 6+ Seconds' THEN fg2_pct ELSE 0 END) AS fg2_pct_touch_6plus_seconds,
SUM(CASE WHEN type = 'Touch 6+ Seconds' THEN fg3a_frequency ELSE 0 END) AS fg3a_frequency_touch_6plus_seconds,
SUM(CASE WHEN type = 'Touch 6+ Seconds' THEN fg3m ELSE 0 END) AS fg3m_touch_6plus_seconds,
SUM(CASE WHEN type = 'Touch 6+ Seconds' THEN fg3a ELSE 0 END) AS fg3a_touch_6plus_seconds,
SUM(CASE WHEN type = 'Touch 6+ Seconds' THEN fg3_pct ELSE 0 END) AS fg3_pct_touch_6plus_seconds
FROM nba_teamdashptshots micro_stats
  INNER JOIN nba_team_pivot piv ON micro_stats.team_id = piv.nba_team_id
  INNER JOIN nba_season_dates s ON micro_stats.game_date BETWEEN s.start_date AND s.end_date
WHERE type NOT IN ('Unknown Touch Seconds', 'Not Captured', 'ShotClock Off')
  AND category NOT IN ('close_def_dist_range10ft_plus')
GROUP BY nba_team_code, game_date, season
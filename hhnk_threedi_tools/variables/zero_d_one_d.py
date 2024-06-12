# -*- coding: utf-8 -*-
"""
Created on Fri Aug 20 16:10:53 2021

@author: chris.kerklaan
"""

# columns
lvl_start_col = "lvl_start"
lvl_end_col = "lvl_end"
lvl_rain_col = "lvl_rain"
lvl_end_rain_col = "lvl_end_rain"

# Hydraulic tests
res_orifices = "orifices"
res_channels = "channels"
res_culverts = "culverts"

start_node_col = "start_node"
end_node_col = "end_node"
code_col = "code"
flow_direction_col = "flow_direction"
slope_col = "slope"
water_lvl_diff_col = "wtr_lvl_diff_m"
q_col = "q"
u_var_col = "u"
slope_abs_cm_km_col = "slope_abs_cm_km"
waterlevel_diff_abs_m_col = "wlvl_diff_abs_m"
map_id_col = "map_id"
zoom_cat_col = "zoom_cat"
upstream_id_col = "upstream_id"
downstream_id_col = "downstream_id"
waterlevel_up_end_col = "wlvl_end_up"
waterlevel_down_end_col = "wlvl_end_down"
waterlevel_up_start_col = "wlvl_start_up"
waterlevel_down_start_col = "wlvl_start_down"
struct_on_lvl_limit_col = "on_lvl_area_limit"
id_col = "id"
index_col = "index"
primary_col = "is_primary"
waterlevel_t_0_col = "wtrlvl_t0"
waterlevel_t_end_col = "wtrlvl_t_end"


norm_lvl_change_before_rain = 0.06  # peil zakt tot begin bui maximaal 6 cm uit (afslagpeil) en stijgt maximaal 6 cm
norm_lvl_stable_end = 0.01  # precies: de opstuwing moet in evenwicht zijn aan het einde van de neerslagperiode\
# Used to check diff in lvl between end and one timestep before end rain
norm_lvl_change_total = (
    0.05  # over de hele simulatie zakt het peil maximaal 5 cm uit (afslagpeil) en stijgt maximaal 5cm
)
norm_impoundment_lvl_25 = 0.25  # de opstuwing tijdens 48u stationare belasting blijft beperkt tot 25 cm boven peil
norm_impoundment_lvl_50 = 0.5  # de opstuwing tijdens 48u stationare belasting blijft beperkt tot 50 cm boven peil

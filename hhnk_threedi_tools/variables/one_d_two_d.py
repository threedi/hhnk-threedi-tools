# -*- coding: utf-8 -*-
"""
Created on Mon Aug 23 11:31:45 2021

@author: chris.kerklaan
"""
# General
id_col = "id"
spatialite_id_col = "spatialite_id"

# Flowlines
content_type_col = "content_type"
kcu_col = "kcu"
q_m3_s_col = "q_m3_s"
vel_m_s_col = "vel_m_s"
pump_capacity_m3_s_col = "pump_capacity_m3_s"

# Nodes
node_type_col = "node_type"
max_area_col = "max_area"
minimal_dem_col = "minimal_dem"
one_d = "1d"
two_d = "2d"
one_d_boundary_col = "1d_boundary"
wtrlvl_m_col = "wtrlvl_m"
wet_area_m2_col = "wet_area_m2"
volume_m3_col = "volume_m3"
storage_mm_col = "storage_mm"

# waterlevel at timesteps
wtrlvl_col = "wtr_lvl"


# definitions
one_d_two_d = "1d2d"
two_d = "2d"
start_rain_sfx = "_start_rain"
end_rain_sfx = "_end_rain"
twelve_hr_after_rain_sfx = "_12_hours_after_rain"
max_sfx = "_max"
suffixes_list = [start_rain_sfx, end_rain_sfx, twelve_hr_after_rain_sfx, max_sfx]

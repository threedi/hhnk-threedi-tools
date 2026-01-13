sql_checks = """
SELECT 'cross_section_location' as table_name,
cross_section_location.id as id,
'WARNING: cross section definition height not used for shape type 1' as error
FROM cross_section_location
WHERE 'cross_section_height' IS NOT NULL AND 'cross_section_shape' = 1
UNION ALL
SELECT 'cross_section_location' as table_name,
cross_section_location.id as id,
'ERROR: multiple height and width entries must have the same count' as error
FROM cross_section_location
WHERE length('cross_section_height') - length(replace('cross_section_height', ' ', '')) <> length('cross_section_width') - length(replace('cross_section_width', ' ', ''))
UNION ALL
SELECT 'surface_map' as table_name,
surface_map.id as id,
'ERROR: impervious surface map refers to non-existent node' as error
FROM surface_map
WHERE surface_map.connection_node_id  NOT IN(
SELECT id
FROM connection_node
)
UNION ALL
SELECT 'surface' as table_name,
surface.id as id,
'WARNING: surface is not in mapping table' as error
FROM surface
WHERE surface.id  NOT IN(
SELECT surface_id
FROM surface_map
)
UNION ALL
SELECT 'surface_map' as table_name,
surface_map.id as id,
'WARNING: surface map is not in surface layer' as error
FROM surface_map
WHERE surface_map.id  NOT IN(
SELECT id
FROM surface
)
UNION ALL
SELECT 'connection_node' as table_name,
connection_node.id as id,
'WARNING: connection node without impervious surface' as error
FROM connection_node
WHERE connection_node.id IN(
SELECT surface_map.connection_node_id
FROM surface_map
WHERE surface_map.surface_id NOT IN(
SELECT surface.id
FROM surface
))
UNION ALL
SELECT 'channel' as table_name,
channel.id as id,
'ERROR: channel without cross section location' as error
FROM channel
LEFT JOIN cross_section_location
ON channel.id = cross_section_location.channel_id
WHERE cross_section_location.id IS NULL
UNION ALL
SELECT 'cross_section_location' as table_name,
cross_section_location.id as id,
'ERROR: cross section location has no definition shape' as error
FROM cross_section_location
WHERE cross_section_shape IS NULL
UNION ALL
SELECT 'culvert' as table_name,
culvert.id as id,
'ERROR: culvert has no definition shape' as error
FROM culvert
WHERE cross_section_shape IS NULL
UNION ALL
SELECT 'weir' as table_name,
weir.id as id,
'ERROR: weir has no definition shape' as error
FROM weir
WHERE cross_section_shape IS NULL
UNION ALL
SELECT 'orifice' as table_name,
orifice.id as id,
'ERROR: orifice has no definition shape' as error
FROM orifice
WHERE cross_section_shape IS NULL
UNION ALL
SELECT 'boundary_condition_1d' as table_name,
boundary_condition_1d.id as id,
'WARNING: 1d boundary without connection node' as error
FROM boundary_condition_1d
LEFT JOIN connection_node
ON boundary_condition_1d.connection_node_id = connection_node.id
WHERE connection_node.id IS NULL
UNION ALL
SELECT 'cross_section_location' as table_name,
cross_section_location.id as id,
'ERROR: cross section location without reference level' as error
FROM cross_section_location
WHERE reference_level IS NULL
UNION ALL
SELECT 'connection_node' as table_name,
connection_node.id as id,
'WARNING: connection node without initial waterlevel' as error
FROM connection_node
WHERE initial_water_level IS NULL
UNION ALL
SELECT 'connection_node' as table_name,
connection_node.id as id,
'ERROR: node without connection' as error
FROM connection_node
WHERE connection_node.id NOT IN (SELECT connection_node_id
FROM pump
WHERE connection_node_id IS NOT NULL
UNION ALL
SELECT connection_node_id_end
FROM pump_map
WHERE connection_node_id_end IS NOT NULL
UNION ALL
SELECT connection_node_id_start
FROM channel
WHERE connection_node_id_start IS NOT NULL
UNION ALL
SELECT connection_node_id_end
FROM channel
WHERE connection_node_id_end IS NOT NULL
UNION ALL
SELECT connection_node_id_start
FROM culvert
WHERE connection_node_id_start IS NOT NULL
UNION ALL
SELECT connection_node_id_end
FROM culvert
WHERE connection_node_id_end IS NOT NULL
UNION ALL
SELECT connection_node_id_start
FROM pipe
WHERE connection_node_id_start IS NOT NULL
UNION ALL
SELECT connection_node_id_end
FROM pipe
WHERE connection_node_id_end IS NOT NULL
UNION ALL
SELECT connection_node_id_start
FROM weir
WHERE connection_node_id_start IS NOT NULL
UNION ALL
SELECT connection_node_id_end
FROM weir
WHERE connection_node_id_end IS NOT NULL
UNION ALL
SELECT connection_node_id_start
FROM orifice
WHERE connection_node_id_start IS NOT NULL
UNION ALL
SELECT connection_node_id_end
FROM orifice
WHERE connection_node_id_end IS NOT NULL
)
UNION ALL
SELECT 'pump' as table_name,
pump.id as id,
'WARNING: start level for pump station not close to initial waterlevel' as error
FROM pump
LEFT JOIN connection_node
ON pump.connection_node_id = connection_node.id
WHERE abs(connection_node.initial_water_level - pump.start_level) > 0.05
UNION ALL
SELECT 'pump' as table_name,
pump.id as id,
'ERROR: pumpstation from and to the same connection node' as error
FROM pump
LEFT JOIN pump_map
ON pump_map.pump_id = pump.id
WHERE pump.connection_node_id = pump_map.connection_node_id_end
UNION ALL
SELECT 'weir' as table_name,
weir.id as id,
'ERROR: weir from and to the same connection node' as error
FROM weir
WHERE weir.connection_node_id_start = weir.connection_node_id_end
UNION ALL
SELECT 'orifice' as table_name,
orifice.id as id,
'ERROR: orifice from and to the same connection node' as error
FROM orifice
WHERE orifice.connection_node_id_start = orifice.connection_node_id_end
UNION ALL
SELECT 'culvert' as table_name,
culvert.id as id,
'ERROR: culvert from and to the same connection node' as error
FROM culvert
WHERE culvert.connection_node_id_start = culvert.connection_node_id_end
UNION ALL
SELECT 'weir' as table_name,
weir.id as id,
'WARNING: start level ' || weir.crest_level || ' not close to initial waterlevel ' || conn1.initial_water_level || ', ' || conn2.initial_water_level as error
FROM weir
LEFT JOIN connection_node as conn1
ON weir.connection_node_id_start = conn1.id
LEFT JOIN connection_node as conn2
ON weir.connection_node_id_end = conn2.id
WHERE abs(conn1.initial_water_level - weir.crest_level) > 0.05 AND abs(conn2.initial_water_level - weir.crest_level) > 0.05 AND weir.crest_level != 15
UNION ALL
SELECT 'channel' as table_name,
channel.id as id,
'WARNING: initial water level at start and end node are not equal ' || conn1.initial_water_level || ', ' || conn2.initial_water_level as error
FROM channel
LEFT JOIN connection_node as conn1
ON channel.connection_node_id_start = conn1.id
LEFT JOIN connection_node as conn2
ON channel.connection_node_id_end = conn2.id
WHERE conn1.initial_water_level != conn2.initial_water_level
UNION ALL
SELECT 'orifice' as table_name,
orifice.id as id,
'WARNING: initial water level at start and end node are not equal ' || conn1.initial_water_level || ', ' || conn2.initial_water_level as error
FROM orifice
LEFT JOIN connection_node as conn1
ON orifice.connection_node_id_start = conn1.id
LEFT JOIN connection_node as conn2
ON orifice.connection_node_id_end = conn2.id
WHERE conn1.initial_water_level != conn2.initial_water_level AND orifice.discharge_coefficient_positive != 0 AND orifice.discharge_coefficient_negative != 0 AND orifice.crest_level < max(conn1.initial_water_level, conn2.initial_water_level)
UNION ALL
SELECT 'culvert' as table_name,
culvert.id as id,
'WARNING: initial water level at start and end node are not equal ' || conn1.initial_water_level || ', ' || conn2.initial_water_level as error
FROM culvert
LEFT JOIN connection_node as conn1
ON culvert.connection_node_id_start = conn1.id
LEFT JOIN connection_node as conn2
ON culvert.connection_node_id_end = conn2.id
WHERE conn1.initial_water_level != conn2.initial_water_level AND culvert.discharge_coefficient_positive != 0 AND culvert.discharge_coefficient_negative != 0 AND max(culvert.invert_level_start, culvert.invert_level_end) < max(conn1.initial_water_level, conn2.initial_water_level)
UNION ALL
SELECT 'surface_map' as table_name,
surface_map.id as id,
'WARNING: percentage = 100 and should be 14.4 or 11.5' as error
FROM surface_map
WHERE surface_map.percentage = 100
UNION ALL
SELECT 'table_control' as table_name,
table_control.id as id,
'ERROR: action_table has more than 1000 characters (model will crash)' as error
FROM table_control
WHERE length(table_control.action_table) > 1000
UNION ALL
SELECT
'table_control' as table_name,
table_control.id as id,
'ERROR: structure control does not work for culverts' as error 
FROM table_control
WHERE action_table IS NOT NULL 
AND target_type LIKE 'culvert%';
"""

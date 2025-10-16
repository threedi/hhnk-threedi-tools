SELECT 'v2_cross_section_definition' as table_name,
v2_cross_section_definition.id as id,
'WARNING: cross section definition height not used for shape type 1' as error
FROM v2_cross_section_definition
WHERE 'height' IS NOT NULL AND 'shape' = 1
UNION ALL
SELECT 'v2_cross_section_definition' as table_name,
v2_cross_section_definition.id as id,
'ERROR: multiple height and width entries must have the same count' as error
FROM v2_cross_section_definition
WHERE length('height') - length(replace('height', ' ', '')) <> length('width') - length(replace('width', ' ', ''))
UNION ALL
SELECT 'v2_impervious_surface' as table_name,
v2_impervious_surface.id as id,
'WARNING: impervious surface is not in mapping table' as error
FROM v2_impervious_surface
WHERE v2_impervious_surface.id  NOT IN(
SELECT impervious_surface_id
FROM v2_impervious_surface_map
)
UNION ALL
SELECT 'v2_impervious_surface_map' as table_name,
v2_impervious_surface_map.id as id,
'ERROR: impervious surface map refers to non-existent node' as error
FROM v2_impervious_surface_map
WHERE v2_impervious_surface_map.connection_node_id  NOT IN(
SELECT id
FROM v2_connection_nodes
)
UNION ALL
SELECT 'v2_surface' as table_name,
v2_surface.id as id,
'WARNING: surface is not in mapping table' as error
FROM v2_surface
WHERE v2_surface.id  NOT IN(
SELECT surface_id
FROM v2_surface_map
)
UNION ALL
SELECT 'v2_impervious_surface_map' as table_name,
v2_impervious_surface_map.id as id,
'WARNING: impervious surface map is not in impervious surface layer' as error
FROM v2_impervious_surface_map
WHERE v2_impervious_surface_map.id  NOT IN(
SELECT id
FROM v2_impervious_surface
)
UNION ALL
SELECT 'v2_connection_nodes' as table_name,
v2_connection_nodes.id as id,
'WARNING: connection node without impervious surface' as error
FROM v2_connection_nodes
WHERE v2_connection_nodes.id IN(
SELECT v2_impervious_surface_map.connection_node_id
FROM v2_impervious_surface_map
WHERE v2_impervious_surface_map.impervious_surface_id NOT IN(
SELECT v2_impervious_surface.id
FROM v2_impervious_surface
))
UNION ALL
SELECT 'v2_channel' as table_name,
v2_channel.id as id,
'ERROR: channel without cross section location' as error
FROM v2_channel
LEFT JOIN v2_cross_section_location
ON v2_channel.id = v2_cross_section_location.channel_id
WHERE v2_cross_section_location.id IS NULL
UNION ALL
SELECT 'v2_cross_section_location' as table_name,
v2_cross_section_location.id as id,
'ERROR: cross section location without definition' as error
FROM v2_cross_section_location
LEFT JOIN v2_cross_section_definition
ON v2_cross_section_location.definition_id = v2_cross_section_definition.id
WHERE v2_cross_section_definition.id IS NULL
UNION ALL
SELECT 'v2_culvert' as table_name,
v2_culvert.id as id,
'ERROR: culvert without cross section definition' as error
FROM v2_culvert
LEFT JOIN v2_cross_section_definition
ON v2_culvert.cross_section_definition_id = v2_cross_section_definition.id
WHERE v2_cross_section_definition.id IS NULL
UNION ALL
SELECT 'v2_weir' as table_name,
v2_weir.id as id,
'ERROR: weir without cross section definition' as error
FROM v2_weir
LEFT JOIN v2_cross_section_definition
ON v2_weir.cross_section_definition_id = v2_cross_section_definition.id
WHERE v2_cross_section_definition.id IS NULL
UNION ALL
SELECT 'v2_orifice' as table_name,
v2_orifice.id as id,
'ERROR: orifice without cross section definition' as error
FROM v2_orifice
LEFT JOIN v2_cross_section_definition
ON v2_orifice.cross_section_definition_id = v2_cross_section_definition.id
WHERE v2_cross_section_definition.id IS NULL
UNION ALL
SELECT 'v2_manhole' as table_name,
v2_manhole.id as id,
'ERROR: manhole without connection node' as error
FROM v2_manhole
LEFT JOIN v2_connection_nodes
ON v2_manhole.connection_node_id = v2_connection_nodes.id
WHERE v2_connection_nodes.id IS NULL
UNION ALL
SELECT 'v2_1d_boundary_conditions' as table_name,
v2_1d_boundary_conditions.id as id,
'WARNING: 1d boundary without connection node' as error
FROM v2_1d_boundary_conditions
LEFT JOIN v2_connection_nodes
ON v2_1d_boundary_conditions.connection_node_id = v2_connection_nodes.id
WHERE v2_connection_nodes.id IS NULL
UNION ALL
SELECT 'v2_cross_section_location' as table_name,
v2_cross_section_location.id as id,
'ERROR: cross section location without reference level' as error
FROM v2_cross_section_location
WHERE reference_level IS NULL
UNION ALL
SELECT 'v2_connection_nodes' as table_name,
v2_connection_nodes.id as id,
'WARNING: connection node without initial waterlevel' as error
FROM v2_connection_nodes
WHERE initial_waterlevel IS NULL
UNION ALL
SELECT 'v2_connection_nodes' as table_name,
v2_connection_nodes.id as id,
'ERROR: node without connection' as error
FROM v2_connection_nodes
WHERE v2_connection_nodes.id NOT IN (SELECT connection_node_start_id
FROM v2_pumpstation
WHERE connection_node_start_id IS NOT NULL
UNION ALL
SELECT connection_node_end_id
FROM v2_pumpstation
WHERE connection_node_end_id IS NOT NULL
UNION ALL
SELECT connection_node_start_id
FROM v2_channel
WHERE connection_node_start_id IS NOT NULL
UNION ALL
SELECT connection_node_end_id
FROM v2_channel
WHERE connection_node_end_id IS NOT NULL
UNION ALL
SELECT connection_node_start_id
FROM v2_culvert
WHERE connection_node_start_id IS NOT NULL
UNION ALL
SELECT connection_node_end_id
FROM v2_culvert
WHERE connection_node_end_id IS NOT NULL
UNION ALL
SELECT connection_node_start_id
FROM v2_pipe
WHERE connection_node_start_id IS NOT NULL
UNION ALL
SELECT connection_node_end_id
FROM v2_pipe
WHERE connection_node_end_id IS NOT NULL
UNION ALL
SELECT connection_node_start_id
FROM v2_weir
WHERE connection_node_start_id IS NOT NULL
UNION ALL
SELECT connection_node_end_id
FROM v2_weir
WHERE connection_node_end_id IS NOT NULL
UNION ALL
SELECT connection_node_start_id
FROM v2_orifice
WHERE connection_node_start_id IS NOT NULL
UNION ALL
SELECT connection_node_end_id
FROM v2_orifice
WHERE connection_node_end_id IS NOT NULL
)
UNION ALL
SELECT 'v2_pumpstation' as table_name,
v2_pumpstation.id as id,
'WARNING: start level for pump station not close to initial waterlevel' as error
FROM v2_pumpstation
LEFT JOIN v2_connection_nodes
ON v2_pumpstation.connection_node_start_id = v2_connection_nodes.id
WHERE abs(v2_connection_nodes.initial_waterlevel - v2_pumpstation.start_level) > 0.05
UNION ALL
SELECT 'v2_cross_section_definition' as table_name,
v2_cross_section_definition.id as id,
'ERROR: shape or width is not defined' as error
FROM v2_cross_section_definition
WHERE shape IS NULL OR width IS NULL OR width = ''
UNION ALL
SELECT 'v2_pumpstation' as table_name,
v2_pumpstation.id as id,
'ERROR: pumpstation from and to the same connection node' as error
FROM v2_pumpstation
WHERE v2_pumpstation.connection_node_start_id = v2_pumpstation.connection_node_end_id
UNION ALL
SELECT 'v2_weir' as table_name,
v2_weir.id as id,
'ERROR: weir from and to the same connection node' as error
FROM v2_weir
WHERE v2_weir.connection_node_start_id = v2_weir.connection_node_end_id
UNION ALL
SELECT 'v2_orifice' as table_name,
v2_orifice.id as id,
'ERROR: orifice from and to the same connection node' as error
FROM v2_orifice
WHERE v2_orifice.connection_node_start_id = v2_orifice.connection_node_end_id
UNION ALL
SELECT 'v2_culvert' as table_name,
v2_culvert.id as id,
'ERROR: culvert from and to the same connection node' as error
FROM v2_culvert
WHERE v2_culvert.connection_node_start_id = v2_culvert.connection_node_end_id
UNION ALL
SELECT 'v2_weir' as table_name,
v2_weir.id as id,
'WARNING: start level ' || v2_weir.crest_level || ' not close to initial waterlevel ' || conn1.initial_waterlevel || ', ' || conn2.initial_waterlevel as error
FROM v2_weir
LEFT JOIN v2_connection_nodes as conn1
ON v2_weir.connection_node_start_id = conn1.id
LEFT JOIN v2_connection_nodes as conn2
ON v2_weir.connection_node_end_id = conn2.id
WHERE abs(conn1.initial_waterlevel - v2_weir.crest_level) > 0.05 AND abs(conn2.initial_waterlevel - v2_weir.crest_level) > 0.05 AND v2_weir.crest_level != 15
UNION ALL
SELECT 'v2_channel' as table_name,
v2_channel.id as id,
'WARNING: initial water level at start and end node are not equal ' || conn1.initial_waterlevel || ', ' || conn2.initial_waterlevel as error
FROM v2_channel
LEFT JOIN v2_connection_nodes as conn1
ON v2_channel.connection_node_start_id = conn1.id
LEFT JOIN v2_connection_nodes as conn2
ON v2_channel.connection_node_end_id = conn2.id
WHERE conn1.initial_waterlevel != conn2.initial_waterlevel
UNION ALL
SELECT 'v2_orifice' as table_name,
v2_orifice.id as id,
'WARNING: initial water level at start and end node are not equal ' || conn1.initial_waterlevel || ', ' || conn2.initial_waterlevel as error
FROM v2_orifice
LEFT JOIN v2_connection_nodes as conn1
ON v2_orifice.connection_node_start_id = conn1.id
LEFT JOIN v2_connection_nodes as conn2
ON v2_orifice.connection_node_end_id = conn2.id
WHERE conn1.initial_waterlevel != conn2.initial_waterlevel AND v2_orifice.discharge_coefficient_positive != 0 AND v2_orifice.discharge_coefficient_negative != 0 AND v2_orifice.crest_level < max(conn1.initial_waterlevel, conn2.initial_waterlevel)
UNION ALL
SELECT 'v2_culvert' as table_name,
v2_culvert.id as id,
'WARNING: initial water level at start and end node are not equal ' || conn1.initial_waterlevel || ', ' || conn2.initial_waterlevel as error
FROM v2_culvert
LEFT JOIN v2_connection_nodes as conn1
ON v2_culvert.connection_node_start_id = conn1.id
LEFT JOIN v2_connection_nodes as conn2
ON v2_culvert.connection_node_end_id = conn2.id
WHERE conn1.initial_waterlevel != conn2.initial_waterlevel AND v2_culvert.discharge_coefficient_positive != 0 AND v2_culvert.discharge_coefficient_negative != 0 AND max(v2_culvert.invert_level_start_point, v2_culvert.invert_level_end_point) < max(conn1.initial_waterlevel, conn2.initial_waterlevel)
UNION ALL
SELECT 'v2_impervious_surface_map' as table_name,
v2_impervious_surface_map.id as id,
'WARNING: percentage = 100 and should be 14.4 or 11.5' as error
FROM v2_impervious_surface_map
WHERE v2_impervious_surface_map.percentage = 100
UNION ALL
SELECT 'v2_control_table' as table_name,
v2_control_table.id as id,
'ERROR: action_table has more than 1000 characters (model will crash)' as error
FROM v2_control_table
WHERE length(v2_control_table.action_table) > 1000


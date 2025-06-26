from hhnk_threedi_tools.core.vergelijkingstool import name_date
from hhnk_threedi_tools.core.vergelijkingstool.name_date import symbology_both

# styling.py

update_symbology = symbology_both(False)

path = name_date.path

model_name, source_data, folder = name_date.name(path)

fn_damo_new = name_date.fn_damo_new
fn_hdb_old = name_date.fn_hdb_old
fn_damo_old = name_date.fn_damo_old
fn_hdb_new = name_date.fn_hdb_new
fn_3di = name_date.fn_threedimodel

date_fn_damo_new, date_fn_damo_old, date_hdb_new, date_hdb_old, date_threedi = name_date.date(
    fn_damo_new, fn_damo_old, fn_hdb_new, fn_hdb_old, fn_3di
)

STYLING_BASIC_TABLE_COLUMNS = [
    "id",
    "f_table_catalog",
    "f_table_schema",
    "f_table_name",
    "f_geometry_column",
    "styleName",
    "styleQML",
    "styleSLD",
    "useAsDefault",
    "description",
    "owner",
    "ui",
    "update_time",
]
# STYLING_POINTS_DAMO = f"""
# <!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
# <qgis version="3.22.4-Białowieża" styleCategories="Symbology">
#   <renderer-v2 symbollevels="0" forceraster="0" enableorderby="0" type="categorizedSymbol" attr="in_both" referencescale="-1">
#     <categories>
#       <category symbol="0" render="true" label="{model_name} new {date_fn_damo_new}" value="{model_name} new"/>
#       <category symbol="1" render="true" label="{model_name} old {date_fn_damo_old}" value="{model_name} old"/>
#       <category symbol="2" render="true" label="{model_name} both" value="{model_name} both"/>
#     </categories>
#     <symbols>
#       <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="marker" name="0">
#         <data_defined_properties>
#           <Option type="Map">
#             <Option value="" type="QString" name="name"/>
#             <Option name="properties"/>
#             <Option value="collection" type="QString" name="type"/>
#           </Option>
#         </data_defined_properties>
#         <layer class="SimpleMarker" pass="0" enabled="1" locked="0">
#           <Option type="Map">
#             <Option value="0" type="QString" name="angle"/>
#             <Option value="square" type="QString" name="cap_style"/>
#             <Option value="255,127,0,255" type="QString" name="color"/>
#             <Option value="1" type="QString" name="horizontal_anchor_point"/>
#             <Option value="bevel" type="QString" name="joinstyle"/>
#             <Option value="circle" type="QString" name="name"/>
#             <Option value="0,0" type="QString" name="offset"/>
#             <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
#             <Option value="MM" type="QString" name="offset_unit"/>
#             <Option value="35,35,35,255" type="QString" name="outline_color"/>
#             <Option value="solid" type="QString" name="outline_style"/>
#             <Option value="0" type="QString" name="outline_width"/>
#             <Option value="3x:0,0,0,0,0,0" type="QString" name="outline_width_map_unit_scale"/>
#             <Option value="MM" type="QString" name="outline_width_unit"/>
#             <Option value="diameter" type="QString" name="scale_method"/>
#             <Option value="2" type="QString" name="size"/>
#             <Option value="3x:0,0,0,0,0,0" type="QString" name="size_map_unit_scale"/>
#             <Option value="MM" type="QString" name="size_unit"/>
#             <Option value="1" type="QString" name="vertical_anchor_point"/>
#           </Option>
#           <prop v="0" k="angle"/>
#           <prop v="square" k="cap_style"/>
#           <prop v="255,127,0,255" k="color"/>
#           <prop v="1" k="horizontal_anchor_point"/>
#           <prop v="bevel" k="joinstyle"/>
#           <prop v="circle" k="name"/>
#           <prop v="0,0" k="offset"/>
#           <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
#           <prop v="MM" k="offset_unit"/>
#           <prop v="35,35,35,255" k="outline_color"/>
#           <prop v="solid" k="outline_style"/>
#           <prop v="0" k="outline_width"/>
#           <prop v="3x:0,0,0,0,0,0" k="outline_width_map_unit_scale"/>
#           <prop v="MM" k="outline_width_unit"/>
#           <prop v="diameter" k="scale_method"/>
#           <prop v="2" k="size"/>
#           <prop v="3x:0,0,0,0,0,0" k="size_map_unit_scale"/>
#           <prop v="MM" k="size_unit"/>
#           <prop v="1" k="vertical_anchor_point"/>
#           <data_defined_properties>
#             <Option type="Map">
#               <Option value="" type="QString" name="name"/>
#               <Option name="properties"/>
#               <Option value="collection" type="QString" name="type"/>
#             </Option>
#           </data_defined_properties>
#         </layer>
#       </symbol>
#       <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="marker" name="1">
#         <data_defined_properties>
#           <Option type="Map">
#             <Option value="" type="QString" name="name"/>
#             <Option name="properties"/>
#             <Option value="collection" type="QString" name="type"/>
#           </Option>
#         </data_defined_properties>
#         <layer class="SimpleMarker" pass="0" enabled="1" locked="0">
#           <Option type="Map">
#             <Option value="0" type="QString" name="angle"/>
#             <Option value="square" type="QString" name="cap_style"/>
#             <Option value="62,131,249,255" type="QString" name="color"/>
#             <Option value="1" type="QString" name="horizontal_anchor_point"/>
#             <Option value="bevel" type="QString" name="joinstyle"/>
#             <Option value="circle" type="QString" name="name"/>
#             <Option value="0,0" type="QString" name="offset"/>
#             <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
#             <Option value="MM" type="QString" name="offset_unit"/>
#             <Option value="35,35,35,255" type="QString" name="outline_color"/>
#             <Option value="solid" type="QString" name="outline_style"/>
#             <Option value="0" type="QString" name="outline_width"/>
#             <Option value="3x:0,0,0,0,0,0" type="QString" name="outline_width_map_unit_scale"/>
#             <Option value="MM" type="QString" name="outline_width_unit"/>
#             <Option value="diameter" type="QString" name="scale_method"/>
#             <Option value="2" type="QString" name="size"/>
#             <Option value="3x:0,0,0,0,0,0" type="QString" name="size_map_unit_scale"/>
#             <Option value="MM" type="QString" name="size_unit"/>
#             <Option value="1" type="QString" name="vertical_anchor_point"/>
#           </Option>
#           <prop v="0" k="angle"/>
#           <prop v="square" k="cap_style"/>
#           <prop v="62,131,249,255" k="color"/>
#           <prop v="1" k="horizontal_anchor_point"/>
#           <prop v="bevel" k="joinstyle"/>
#           <prop v="circle" k="name"/>
#           <prop v="0,0" k="offset"/>
#           <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
#           <prop v="MM" k="offset_unit"/>
#           <prop v="35,35,35,255" k="outline_color"/>
#           <prop v="solid" k="outline_style"/>
#           <prop v="0" k="outline_width"/>
#           <prop v="3x:0,0,0,0,0,0" k="outline_width_map_unit_scale"/>
#           <prop v="MM" k="outline_width_unit"/>
#           <prop v="diameter" k="scale_method"/>
#           <prop v="2" k="size"/>
#           <prop v="3x:0,0,0,0,0,0" k="size_map_unit_scale"/>
#           <prop v="MM" k="size_unit"/>
#           <prop v="1" k="vertical_anchor_point"/>
#           <data_defined_properties>
#             <Option type="Map">
#               <Option value="" type="QString" name="name"/>
#               <Option name="properties"/>
#               <Option value="collection" type="QString" name="type"/>
#             </Option>
#           </data_defined_properties>
#         </layer>
#       </symbol>
#       <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="marker" name="2">
#         <data_defined_properties>
#           <Option type="Map">
#             <Option value="" type="QString" name="name"/>
#             <Option name="properties"/>
#             <Option value="collection" type="QString" name="type"/>
#           </Option>
#         </data_defined_properties>
#         <layer class="SimpleMarker" pass="0" enabled="1" locked="0">
#           <Option type="Map">
#             <Option value="0" type="QString" name="angle"/>
#             <Option value="square" type="QString" name="cap_style"/>
#             <Option value="197,197,197,{update_symbology}" type="QString" name="color"/>
#             <Option value="1" type="QString" name="horizontal_anchor_point"/>
#             <Option value="bevel" type="QString" name="joinstyle"/>
#             <Option value="circle" type="QString" name="name"/>
#             <Option value="0,0" type="QString" name="offset"/>
#             <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
#             <Option value="MM" type="QString" name="offset_unit"/>
#             <Option value="35,35,35,{update_symbology}" type="QString" name="outline_color"/>
#             <Option value="solid" type="QString" name="outline_style"/>
#             <Option value="0" type="QString" name="outline_width"/>
#             <Option value="3x:0,0,0,0,0,0" type="QString" name="outline_width_map_unit_scale"/>
#             <Option value="MM" type="QString" name="outline_width_unit"/>
#             <Option value="diameter" type="QString" name="scale_method"/>
#             <Option value="2" type="QString" name="size"/>
#             <Option value="3x:0,0,0,0,0,0" type="QString" name="size_map_unit_scale"/>
#             <Option value="MM" type="QString" name="size_unit"/>
#             <Option value="1" type="QString" name="vertical_anchor_point"/>
#           </Option>
#           <prop v="0" k="angle"/>
#           <prop v="square" k="cap_style"/>
#           <prop v="197,197,197,{update_symbology}" k="color"/>
#           <prop v="1" k="horizontal_anchor_point"/>
#           <prop v="bevel" k="joinstyle"/>
#           <prop v="circle" k="name"/>
#           <prop v="0,0" k="offset"/>
#           <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
#           <prop v="MM" k="offset_unit"/>
#           <prop v="35,35,35,{update_symbology}" k="outline_color"/>
#           <prop v="solid" k="outline_style"/>
#           <prop v="0" k="outline_width"/>
#           <prop v="3x:0,0,0,0,0,0" k="outline_width_map_unit_scale"/>
#           <prop v="MM" k="outline_width_unit"/>
#           <prop v="diameter" k="scale_method"/>
#           <prop v="2" k="size"/>
#           <prop v="3x:0,0,0,0,0,0" k="size_map_unit_scale"/>
#           <prop v="MM" k="size_unit"/>
#           <prop v="1" k="vertical_anchor_point"/>
#           <data_defined_properties>
#             <Option type="Map">
#             <Option name="name" value="" type="QString"/>
#             <Option name="properties" type="Map">
#               <Option name="outlineStyle" type="Map">
#               <Option name="active" value="true" type="bool"/>
#               <Option name="expression" value="if(&quot;number_of_critical&quot;=0,'no','solid')" type="QString"/>
#               <Option name="type" value="3" type="int"/>
#               </Option>
#               <Option name="outlineWidth" type="Map">
#               <Option name="active" value="true" type="bool"/>
#               <Option name="field" value="number_of_critical" type="QString"/>
#               <Option name="type" value="2" type="int"/>
#               </Option>
#             </Option>
#             <Option name="type" value="collection" type="QString"/>
#             </Option>
#           </data_defined_properties>
#         </layer>
#       </symbol>
#     </symbols>
#     <source-symbol>
#       <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="marker" name="0">
#         <data_defined_properties>
#           <Option type="Map">
#             <Option value="" type="QString" name="name"/>
#             <Option name="properties"/>
#             <Option value="collection" type="QString" name="type"/>
#           </Option>
#         </data_defined_properties>
#         <layer class="SimpleMarker" pass="0" enabled="1" locked="0">
#           <Option type="Map">
#             <Option value="0" type="QString" name="angle"/>
#             <Option value="square" type="QString" name="cap_style"/>
#             <Option value="164,113,88,255" type="QString" name="color"/>
#             <Option value="1" type="QString" name="horizontal_anchor_point"/>
#             <Option value="bevel" type="QString" name="joinstyle"/>
#             <Option value="circle" type="QString" name="name"/>
#             <Option value="0,0" type="QString" name="offset"/>
#             <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
#             <Option value="MM" type="QString" name="offset_unit"/>
#             <Option value="35,35,35,255" type="QString" name="outline_color"/>
#             <Option value="solid" type="QString" name="outline_style"/>
#             <Option value="0" type="QString" name="outline_width"/>
#             <Option value="3x:0,0,0,0,0,0" type="QString" name="outline_width_map_unit_scale"/>
#             <Option value="MM" type="QString" name="outline_width_unit"/>
#             <Option value="diameter" type="QString" name="scale_method"/>
#             <Option value="2" type="QString" name="size"/>
#             <Option value="3x:0,0,0,0,0,0" type="QString" name="size_map_unit_scale"/>
#             <Option value="MM" type="QString" name="size_unit"/>
#             <Option value="1" type="QString" name="vertical_anchor_point"/>
#           </Option>
#           <prop v="0" k="angle"/>
#           <prop v="square" k="cap_style"/>
#           <prop v="164,113,88,255" k="color"/>
#           <prop v="1" k="horizontal_anchor_point"/>
#           <prop v="bevel" k="joinstyle"/>
#           <prop v="circle" k="name"/>
#           <prop v="0,0" k="offset"/>
#           <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
#           <prop v="MM" k="offset_unit"/>
#           <prop v="35,35,35,255" k="outline_color"/>
#           <prop v="solid" k="outline_style"/>
#           <prop v="0" k="outline_width"/>
#           <prop v="3x:0,0,0,0,0,0" k="outline_width_map_unit_scale"/>
#           <prop v="MM" k="outline_width_unit"/>
#           <prop v="diameter" k="scale_method"/>
#           <prop v="2" k="size"/>
#           <prop v="3x:0,0,0,0,0,0" k="size_map_unit_scale"/>
#           <prop v="MM" k="size_unit"/>
#           <prop v="1" k="vertical_anchor_point"/>
#           <data_defined_properties>
#             <Option type="Map">
#               <Option value="" type="QString" name="name"/>
#               <Option name="properties"/>
#               <Option value="collection" type="QString" name="type"/>
#             </Option>
#           </data_defined_properties>
#         </layer>
#       </symbol>
#     </source-symbol>
#     <rotation/>
#     <sizescale/>
#   </renderer-v2>
#   <blendMode>0</blendMode>
#   <featureBlendMode>0</featureBlendMode>
#   <layerGeometryType>0</layerGeometryType>
# </qgis>
# """

STYLING_LINES_DAMO = f"""
<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.22.4-Białowieża" styleCategories="Symbology">
  <renderer-v2 symbollevels="0" forceraster="0" enableorderby="0" type="categorizedSymbol" attr="in_both" referencescale="-1">
    <categories>
      <category symbol="0" render="true" label="{model_name} new {date_fn_damo_new}" value="{model_name} new"/>
      <category symbol="1" render="true" label="{model_name} old {date_fn_damo_old}" value="{model_name} old"/>
      <category symbol="2" render="true" label="{model_name} both" value="{model_name} both"/>
    </categories>
    <symbols>
      <symbol name="0" is_animated="0" type="line" frame_rate="10" alpha="1" force_rhr="0" clip_to_extent="1">
    <data_defined_properties>
     <Option type="Map">
      <Option name="name" value="" type="QString"/>
      <Option name="properties"/>
      <Option name="type" value="collection" type="QString"/>
     </Option>
    </data_defined_properties>
    <layer pass="0" enabled="1" class="SimpleLine" locked="0">
     <Option type="Map">
      <Option name="align_dash_pattern" value="0" type="QString"/>
      <Option name="capstyle" value="square" type="QString"/>
      <Option name="customdash" value="5;2" type="QString"/>
      <Option name="customdash_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="customdash_unit" value="MM" type="QString"/>
      <Option name="dash_pattern_offset" value="0" type="QString"/>
      <Option name="dash_pattern_offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="dash_pattern_offset_unit" value="MM" type="QString"/>
      <Option name="draw_inside_polygon" value="0" type="QString"/>
      <Option name="joinstyle" value="bevel" type="QString"/>
      <Option name="line_color" value="255,127,0,255" type="QString"/>
      <Option name="line_style" value="solid" type="QString"/>
      <Option name="line_width" value="1" type="QString"/>
      <Option name="line_width_unit" value="MM" type="QString"/>
      <Option name="offset" value="0" type="QString"/>
      <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="offset_unit" value="MM" type="QString"/>
      <Option name="ring_filter" value="0" type="QString"/>
      <Option name="trim_distance_end" value="0" type="QString"/>
      <Option name="trim_distance_end_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="trim_distance_end_unit" value="MM" type="QString"/>
      <Option name="trim_distance_start" value="0" type="QString"/>
      <Option name="trim_distance_start_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="trim_distance_start_unit" value="MM" type="QString"/>
      <Option name="tweak_dash_pattern_on_corners" value="0" type="QString"/>
      <Option name="use_custom_dash" value="0" type="QString"/>
      <Option name="width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
     </Option>
     <data_defined_properties>
      <Option type="Map">
       <Option name="name" value="" type="QString"/>
       <Option name="properties"/>
       <Option name="type" value="collection" type="QString"/>
      </Option>
     </data_defined_properties>
    </layer>
   </symbol>
   <symbol name="1" is_animated="0" type="line" frame_rate="10" alpha="1" force_rhr="0" clip_to_extent="1">
    <data_defined_properties>
     <Option type="Map">
      <Option name="name" value="" type="QString"/>
      <Option name="properties"/>
      <Option name="type" value="collection" type="QString"/>
     </Option>
    </data_defined_properties>
    <layer pass="0" enabled="1" class="SimpleLine" locked="0">
     <Option type="Map">
      <Option name="align_dash_pattern" value="0" type="QString"/>
      <Option name="capstyle" value="square" type="QString"/>
      <Option name="customdash" value="5;2" type="QString"/>
      <Option name="customdash_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="customdash_unit" value="MM" type="QString"/>
      <Option name="dash_pattern_offset" value="0" type="QString"/>
      <Option name="dash_pattern_offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="dash_pattern_offset_unit" value="MM" type="QString"/>
      <Option name="draw_inside_polygon" value="0" type="QString"/>
      <Option name="joinstyle" value="bevel" type="QString"/>
      <Option name="line_color" value="62,131,249,255" type="QString"/>
      <Option name="line_style" value="solid" type="QString"/>
      <Option name="line_width" value="1" type="QString"/>
      <Option name="line_width_unit" value="MM" type="QString"/>
      <Option name="offset" value="0" type="QString"/>
      <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="offset_unit" value="MM" type="QString"/>
      <Option name="ring_filter" value="0" type="QString"/>
      <Option name="trim_distance_end" value="0" type="QString"/>
      <Option name="trim_distance_end_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="trim_distance_end_unit" value="MM" type="QString"/>
      <Option name="trim_distance_start" value="0" type="QString"/>
      <Option name="trim_distance_start_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="trim_distance_start_unit" value="MM" type="QString"/>
      <Option name="tweak_dash_pattern_on_corners" value="0" type="QString"/>
      <Option name="use_custom_dash" value="0" type="QString"/>
      <Option name="width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
     </Option>
     <data_defined_properties>
      <Option type="Map">
       <Option name="name" value="" type="QString"/>
       <Option name="properties"/>
       <Option name="type" value="collection" type="QString"/>
      </Option>
     </data_defined_properties>
    </layer>
   </symbol>
   <symbol name="2" is_animated="0" type="line" frame_rate="10" alpha="1" force_rhr="0" clip_to_extent="1">
    <data_defined_properties>
     <Option type="Map">
      <Option name="name" value="" type="QString"/>
      <Option name="properties"/>
      <Option name="type" value="collection" type="QString"/>
     </Option>
    </data_defined_properties>
    <layer pass="0" enabled="1" class="SimpleLine" locked="0">
     <Option type="Map">
      <Option name="align_dash_pattern" value="0" type="QString"/>
      <Option name="capstyle" value="square" type="QString"/>
      <Option name="customdash" value="5;2" type="QString"/>
      <Option name="customdash_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="customdash_unit" value="MM" type="QString"/>
      <Option name="dash_pattern_offset" value="0" type="QString"/>
      <Option name="dash_pattern_offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="dash_pattern_offset_unit" value="MM" type="QString"/>
      <Option name="draw_inside_polygon" value="0" type="QString"/>
      <Option name="joinstyle" value="bevel" type="QString"/>
      <Option name="line_color" value="255,1,0,255" type="QString"/>
      <Option name="line_style" value="solid" type="QString"/>
      <Option name="line_width" value="0" type="QString"/>
      <Option name="line_width_unit" value="MM" type="QString"/>
      <Option name="offset" value="0" type="QString"/>
      <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="offset_unit" value="MM" type="QString"/>
      <Option name="ring_filter" value="0" type="QString"/>
      <Option name="trim_distance_end" value="0" type="QString"/>
      <Option name="trim_distance_end_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="trim_distance_end_unit" value="MM" type="QString"/>
      <Option name="trim_distance_start" value="0" type="QString"/>
      <Option name="trim_distance_start_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="trim_distance_start_unit" value="MM" type="QString"/>
      <Option name="tweak_dash_pattern_on_corners" value="0" type="QString"/>
      <Option name="use_custom_dash" value="0" type="QString"/>
      <Option name="width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
     </Option>
     <data_defined_properties>
      <Option type="Map">
       <Option name="name" value="" type="QString"/>
       <Option name="properties" type="Map">
        <Option name="outlineStyle" type="Map">
         <Option name="active" value="true" type="bool"/>
         <Option name="expression" value="if(&quot;number_of_critical&quot;=0,'no','solid')" type="QString"/>
         <Option name="type" value="3" type="int"/>
        </Option>
        <Option name="outlineWidth" type="Map">
         <Option name="active" value="true" type="bool"/>
         <Option name="expression" value="&quot;number_of_critical&quot;" type="QString"/>
         <Option name="type" value="3" type="int"/>
        </Option>
       </Option>
       <Option name="type" value="collection" type="QString"/>
      </Option>
     </data_defined_properties>
    </layer>
    <layer pass="0" enabled="1" class="SimpleLine" locked="0">
     <Option type="Map">
      <Option name="align_dash_pattern" value="0" type="QString"/>
      <Option name="capstyle" value="square" type="QString"/>
      <Option name="customdash" value="5;2" type="QString"/>
      <Option name="customdash_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="customdash_unit" value="MM" type="QString"/>
      <Option name="dash_pattern_offset" value="0" type="QString"/>
      <Option name="dash_pattern_offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="dash_pattern_offset_unit" value="MM" type="QString"/>
      <Option name="draw_inside_polygon" value="0" type="QString"/>
      <Option name="joinstyle" value="bevel" type="QString"/>
      <Option name="line_color" value="197,197,197,{update_symbology}" type="QString"/>
      <Option name="line_style" value="solid" type="QString"/>
      <Option name="line_width" value="1" type="QString"/>
      <Option name="line_width_unit" value="MM" type="QString"/>
      <Option name="offset" value="0" type="QString"/>
      <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="offset_unit" value="MM" type="QString"/>
      <Option name="ring_filter" value="0" type="QString"/>
      <Option name="trim_distance_end" value="0" type="QString"/>
      <Option name="trim_distance_end_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="trim_distance_end_unit" value="MM" type="QString"/>
      <Option name="trim_distance_start" value="0" type="QString"/>
      <Option name="trim_distance_start_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="trim_distance_start_unit" value="MM" type="QString"/>
      <Option name="tweak_dash_pattern_on_corners" value="0" type="QString"/>
      <Option name="use_custom_dash" value="0" type="QString"/>
      <Option name="width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
     </Option>
     <data_defined_properties>
      <Option type="Map">
       <Option name="name" value="" type="QString"/>
       <Option name="properties"/>
       <Option name="type" value="collection" type="QString"/>
      </Option>
     </data_defined_properties>
    </layer>
   </symbol>
  </symbols>
  <source-symbol>
   <symbol name="0" is_animated="0" type="line" frame_rate="10" alpha="1" force_rhr="0" clip_to_extent="1">
    <data_defined_properties>
     <Option type="Map">
      <Option name="name" value="" type="QString"/>
      <Option name="properties"/>
      <Option name="type" value="collection" type="QString"/>
     </Option>
    </data_defined_properties>
    <layer pass="0" enabled="1" class="SimpleLine" locked="0">
     <Option type="Map">
      <Option name="align_dash_pattern" value="0" type="QString"/>
      <Option name="capstyle" value="square" type="QString"/>
      <Option name="customdash" value="5;2" type="QString"/>
      <Option name="customdash_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="customdash_unit" value="MM" type="QString"/>
      <Option name="dash_pattern_offset" value="0" type="QString"/>
      <Option name="dash_pattern_offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="dash_pattern_offset_unit" value="MM" type="QString"/>
      <Option name="draw_inside_polygon" value="0" type="QString"/>
      <Option name="joinstyle" value="bevel" type="QString"/>
      <Option name="line_color" value="213,180,60,255" type="QString"/>
      <Option name="line_style" value="solid" type="QString"/>
      <Option name="line_width" value="1" type="QString"/>
      <Option name="line_width_unit" value="MM" type="QString"/>
      <Option name="offset" value="0" type="QString"/>
      <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="offset_unit" value="MM" type="QString"/>
      <Option name="ring_filter" value="0" type="QString"/>
      <Option name="trim_distance_end" value="0" type="QString"/>
      <Option name="trim_distance_end_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="trim_distance_end_unit" value="MM" type="QString"/>
      <Option name="trim_distance_start" value="0" type="QString"/>
      <Option name="trim_distance_start_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="trim_distance_start_unit" value="MM" type="QString"/>
      <Option name="tweak_dash_pattern_on_corners" value="0" type="QString"/>
      <Option name="use_custom_dash" value="0" type="QString"/>
      <Option name="width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
     </Option>
     <data_defined_properties>
      <Option type="Map">
       <Option name="name" value="" type="QString"/>
       <Option name="properties"/>
       <Option name="type" value="collection" type="QString"/>
      </Option>
     </data_defined_properties>
    </layer>
   </symbol>
  </source-symbol>
  <rotation/>
  <sizescale/>
 </renderer-v2>
 <selection mode="Default">
  <selectionColor invalid="1"/>
 </selection>
 <blendMode>0</blendMode>
 <featureBlendMode>0</featureBlendMode>
 <layerGeometryType>1</layerGeometryType>
</qgis>
"""
STYLING_POINTS_DAMO = f"""

<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis styleCategories="Symbology" version="3.34.4-Prizren">
 <renderer-v2 attr="in_both" forceraster="0" referencescale="-1" symbollevels="0" type="categorizedSymbol" enableorderby="0">
    <categories>
      <category symbol="0" render="true" label="{model_name} new {date_fn_damo_new}" value="{model_name} new"/>
      <category symbol="1" render="true" label="{model_name} old {date_fn_damo_old}" value="{model_name} old"/>
      <category symbol="2" render="true" label="{model_name} both" value="{model_name} both"/>
    </categories>
  <symbols>
   <symbol name="0" is_animated="0" type="marker" frame_rate="10" alpha="1" force_rhr="0" clip_to_extent="1">
    <data_defined_properties>
     <Option type="Map">
      <Option name="name" value="" type="QString"/>
      <Option name="properties"/>
      <Option name="type" value="collection" type="QString"/>
     </Option>
    </data_defined_properties>
    <layer pass="0" enabled="1" class="SimpleMarker" locked="0">
     <Option type="Map">
      <Option name="angle" value="0" type="QString"/>
      <Option name="cap_style" value="square" type="QString"/>
      <Option name="color" value="255,127,0,255" type="QString"/>
      <Option name="horizontal_anchor_point" value="1" type="QString"/>
      <Option name="joinstyle" value="bevel" type="QString"/>
      <Option name="name" value="circle" type="QString"/>
      <Option name="offset" value="0,0" type="QString"/>
      <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="offset_unit" value="MM" type="QString"/>
      <Option name="outline_color" value="35,35,35,255" type="QString"/>
      <Option name="outline_style" value="solid" type="QString"/>
      <Option name="outline_width" value="0" type="QString"/>
      <Option name="outline_width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="outline_width_unit" value="MM" type="QString"/>
      <Option name="scale_method" value="diameter" type="QString"/>
      <Option name="size" value="2" type="QString"/>
      <Option name="size_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="size_unit" value="MM" type="QString"/>
      <Option name="vertical_anchor_point" value="1" type="QString"/>
     </Option>
     <data_defined_properties>
      <Option type="Map">
       <Option name="name" value="" type="QString"/>
       <Option name="properties"/>
       <Option name="type" value="collection" type="QString"/>
      </Option>
     </data_defined_properties>
    </layer>
   </symbol>
   <symbol name="1" is_animated="0" type="marker" frame_rate="10" alpha="1" force_rhr="0" clip_to_extent="1">
    <data_defined_properties>
     <Option type="Map">
      <Option name="name" value="" type="QString"/>
      <Option name="properties"/>
      <Option name="type" value="collection" type="QString"/>
     </Option>
    </data_defined_properties>
    <layer pass="0" enabled="1" class="SimpleMarker" locked="0">
     <Option type="Map">
      <Option name="angle" value="0" type="QString"/>
      <Option name="cap_style" value="square" type="QString"/>
      <Option name="color" value="62,131,249,255" type="QString"/>
      <Option name="horizontal_anchor_point" value="1" type="QString"/>
      <Option name="joinstyle" value="bevel" type="QString"/>
      <Option name="name" value="circle" type="QString"/>
      <Option name="offset" value="0,0" type="QString"/>
      <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="offset_unit" value="MM" type="QString"/>
      <Option name="outline_color" value="35,35,35,255" type="QString"/>
      <Option name="outline_style" value="dash" type="QString"/>
      <Option name="outline_width" value="0" type="QString"/>
      <Option name="outline_width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="outline_width_unit" value="MM" type="QString"/>
      <Option name="scale_method" value="diameter" type="QString"/>
      <Option name="size" value="0.8" type="QString"/>
      <Option name="size_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="size_unit" value="MM" type="QString"/>
      <Option name="vertical_anchor_point" value="1" type="QString"/>
     </Option>
     <data_defined_properties>
      <Option type="Map">
       <Option name="name" value="" type="QString"/>
       <Option name="properties"/>
       <Option name="type" value="collection" type="QString"/>
      </Option>
     </data_defined_properties>
    </layer>
    <layer pass="0" enabled="1" class="SimpleMarker" locked="0">
     <Option type="Map">
      <Option name="angle" value="0" type="QString"/>
      <Option name="cap_style" value="square" type="QString"/>
      <Option name="color" value="62,131,249,255" type="QString"/>
      <Option name="horizontal_anchor_point" value="1" type="QString"/>
      <Option name="joinstyle" value="bevel" type="QString"/>
      <Option name="name" value="circle" type="QString"/>
      <Option name="offset" value="0,0" type="QString"/>
      <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="offset_unit" value="MM" type="QString"/>
      <Option name="outline_color" value="35,35,35,128" type="QString"/>
      <Option name="outline_style" value="solid" type="QString"/>
      <Option name="outline_width" value="0" type="QString"/>
      <Option name="outline_width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="outline_width_unit" value="MM" type="QString"/>
      <Option name="scale_method" value="diameter" type="QString"/>
      <Option name="size" value="2" type="QString"/>
      <Option name="size_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="size_unit" value="MM" type="QString"/>
      <Option name="vertical_anchor_point" value="1" type="QString"/>
     </Option>
     <data_defined_properties>
      <Option type="Map">
       <Option name="name" value="" type="QString"/>
       <Option name="properties"/>
       <Option name="type" value="collection" type="QString"/>
      </Option>
     </data_defined_properties>
    </layer>
   </symbol>
   <symbol name="2" is_animated="0" type="marker" frame_rate="10" alpha="1" force_rhr="0" clip_to_extent="1">
    <data_defined_properties>
     <Option type="Map">
      <Option name="name" value="" type="QString"/>
      <Option name="properties"/>
      <Option name="type" value="collection" type="QString"/>
     </Option>
    </data_defined_properties>
    <layer pass="0" enabled="1" class="SimpleMarker" locked="0">
     <Option type="Map">
      <Option name="angle" value="0" type="QString"/>
      <Option name="cap_style" value="square" type="QString"/>
      <Option name="color" value="255,0,0,0" type="QString"/>
      <Option name="horizontal_anchor_point" value="1" type="QString"/>
      <Option name="joinstyle" value="bevel" type="QString"/>
      <Option name="name" value="circle" type="QString"/>
      <Option name="offset" value="0,0" type="QString"/>
      <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="offset_unit" value="MM" type="QString"/>
      <Option name="outline_color" value="248,0,29,252" type="QString"/>
      <Option name="outline_style" value="solid" type="QString"/>
      <Option name="outline_width" value="0.6" type="QString"/>
      <Option name="outline_width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="outline_width_unit" value="MM" type="QString"/>
      <Option name="scale_method" value="diameter" type="QString"/>
      <Option name="size" value="2" type="QString"/>
      <Option name="size_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="size_unit" value="MM" type="QString"/>
      <Option name="vertical_anchor_point" value="1" type="QString"/>
     </Option>
     <data_defined_properties>
      <Option type="Map">
       <Option name="name" value="" type="QString"/>
       <Option name="properties" type="Map">
        <Option name="outlineStyle" type="Map">
         <Option name="active" value="true" type="bool"/>
         <Option name="expression" value="if(&quot;number_of_critical&quot;=0,'no','solid')" type="QString"/>
         <Option name="type" value="3" type="int"/>
        </Option>
        <Option name="outlineWidth" type="Map">
         <Option name="active" value="true" type="bool"/>
         <Option name="field" value="number_of_critical" type="QString"/>
         <Option name="type" value="2" type="int"/>
        </Option>
       </Option>
       <Option name="type" value="collection" type="QString"/>
      </Option>
     </data_defined_properties>
    </layer>
    <layer pass="0" enabled="1" class="SimpleMarker" locked="0">
     <Option type="Map">
      <Option name="angle" value="0" type="QString"/>
      <Option name="cap_style" value="square" type="QString"/>
      <Option name="color" value="197,197,197, {update_symbology}" type="QString"/>
      <Option name="horizontal_anchor_point" value="1" type="QString"/>
      <Option name="joinstyle" value="bevel" type="QString"/>
      <Option name="name" value="circle" type="QString"/>
      <Option name="offset" value="0,0" type="QString"/>
      <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="offset_unit" value="MM" type="QString"/>
      <Option name="outline_color" value="35,35,35,128" type="QString"/>
      <Option name="outline_style" value="solid" type="QString"/>
      <Option name="outline_width" value="0" type="QString"/>
      <Option name="outline_width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="outline_width_unit" value="MM" type="QString"/>
      <Option name="scale_method" value="diameter" type="QString"/>
      <Option name="size" value="2" type="QString"/>
      <Option name="size_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="size_unit" value="MM" type="QString"/>
      <Option name="vertical_anchor_point" value="1" type="QString"/>
     </Option>
     <data_defined_properties>
      <Option type="Map">
       <Option name="name" value="" type="QString"/>
       <Option name="properties"/>
       <Option name="type" value="collection" type="QString"/>
      </Option>
     </data_defined_properties>
    </layer>
   </symbol>
  </symbols>
  <source-symbol>
   <symbol name="0" is_animated="0" type="marker" frame_rate="10" alpha="1" force_rhr="0" clip_to_extent="1">
    <data_defined_properties>
     <Option type="Map">
      <Option name="name" value="" type="QString"/>
      <Option name="properties"/>
      <Option name="type" value="collection" type="QString"/>
     </Option>
    </data_defined_properties>
    <layer pass="0" enabled="1" class="SimpleMarker" locked="0">
     <Option type="Map">
      <Option name="angle" value="0" type="QString"/>
      <Option name="cap_style" value="square" type="QString"/>
      <Option name="color" value="164,113,88,255" type="QString"/>
      <Option name="horizontal_anchor_point" value="1" type="QString"/>
      <Option name="joinstyle" value="bevel" type="QString"/>
      <Option name="name" value="circle" type="QString"/>
      <Option name="offset" value="0,0" type="QString"/>
      <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="offset_unit" value="MM" type="QString"/>
      <Option name="outline_color" value="35,35,35,255" type="QString"/>
      <Option name="outline_style" value="solid" type="QString"/>
      <Option name="outline_width" value="0" type="QString"/>
      <Option name="outline_width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="outline_width_unit" value="MM" type="QString"/>
      <Option name="scale_method" value="diameter" type="QString"/>
      <Option name="size" value="2" type="QString"/>
      <Option name="size_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
      <Option name="size_unit" value="MM" type="QString"/>
      <Option name="vertical_anchor_point" value="1" type="QString"/>
     </Option>
     <data_defined_properties>
      <Option type="Map">
       <Option name="name" value="" type="QString"/>
       <Option name="properties"/>
       <Option name="type" value="collection" type="QString"/>
      </Option>
     </data_defined_properties>
    </layer>
   </symbol>
  </source-symbol>
  <rotation/>
  <sizescale/>
 </renderer-v2>
 <selection mode="Default">
  <selectionColor invalid="1"/>
 </selection>
 <blendMode>0</blendMode>
 <featureBlendMode>0</featureBlendMode>
 <layerGeometryType>0</layerGeometryType>
</qgis>
"""

STYLING_POLYGONS_DAMO = f"""
<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.22.4-Białowieża" styleCategories="Symbology">
  <renderer-v2 symbollevels="0" forceraster="0" enableorderby="0" type="categorizedSymbol" attr="in_both" referencescale="-1">
    <categories>
      <category symbol="0" render="true" label="{model_name} new {date_fn_damo_new}" value="{model_name} new"/>
      <category symbol="1" render="true" label="{model_name} old {date_fn_damo_old}" value="{model_name} old"/>
      <category symbol="2" render="true" label="{model_name} both" value="{model_name} both"/>
    </categories>
    <symbols>
      <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="fill" name="0">
        <data_defined_properties>
          <Option type="Map">
            <Option value="" type="QString" name="name"/>
            <Option name="properties"/>
            <Option value="collection" type="QString" name="type"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleFill" pass="0" enabled="1" locked="0">
          <Option type="Map">
            <Option value="3x:0,0,0,0,0,0" type="QString" name="border_width_map_unit_scale"/>
            <Option value="255,127,0,255" type="QString" name="color"/>
            <Option value="bevel" type="QString" name="joinstyle"/>
            <Option value="0,0" type="QString" name="offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="offset_unit"/>
            <Option value="35,35,35,255" type="QString" name="outline_color"/>
            <Option value="solid" type="QString" name="outline_style"/>
            <Option value="0.26" type="QString" name="outline_width"/>
            <Option value="MM" type="QString" name="outline_width_unit"/>
            <Option value="solid" type="QString" name="style"/>
          </Option>
          <prop v="3x:0,0,0,0,0,0" k="border_width_map_unit_scale"/>
          <prop v="255,127,0,255" k="color"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="0,0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="35,35,35,255" k="outline_color"/>
          <prop v="solid" k="outline_style"/>
          <prop v="0.26" k="outline_width"/>
          <prop v="MM" k="outline_width_unit"/>
          <prop v="solid" k="style"/>
          <data_defined_properties>
            <Option type="Map">
              <Option value="" type="QString" name="name"/>
              <Option name="properties"/>
              <Option value="collection" type="QString" name="type"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="fill" name="1">
        <data_defined_properties>
          <Option type="Map">
            <Option value="" type="QString" name="name"/>
            <Option name="properties"/>
            <Option value="collection" type="QString" name="type"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleFill" pass="0" enabled="1" locked="0">
          <Option type="Map">
            <Option value="3x:0,0,0,0,0,0" type="QString" name="border_width_map_unit_scale"/>
            <Option value="62,131,249,255" type="QString" name="color"/>
            <Option value="bevel" type="QString" name="joinstyle"/>
            <Option value="0,0" type="QString" name="offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="offset_unit"/>
            <Option value="35,35,35,255" type="QString" name="outline_color"/>
            <Option value="solid" type="QString" name="outline_style"/>
            <Option value="0.26" type="QString" name="outline_width"/>
            <Option value="MM" type="QString" name="outline_width_unit"/>
            <Option value="solid" type="QString" name="style"/>
          </Option>
          <prop v="3x:0,0,0,0,0,0" k="border_width_map_unit_scale"/>
          <prop v="62,131,249,255" k="color"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="0,0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="35,35,35,255" k="outline_color"/>
          <prop v="solid" k="outline_style"/>
          <prop v="0.26" k="outline_width"/>
          <prop v="MM" k="outline_width_unit"/>
          <prop v="solid" k="style"/>
          <data_defined_properties>
            <Option type="Map">
              <Option value="" type="QString" name="name"/>
              <Option name="properties"/>
              <Option value="collection" type="QString" name="type"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="fill" name="2">
        <data_defined_properties>
          <Option type="Map">
            <Option value="" type="QString" name="name"/>
            <Option name="properties"/>
            <Option value="collection" type="QString" name="type"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleFill" pass="0" enabled="1" locked="0">
          <Option type="Map">
            <Option value="3x:0,0,0,0,0,0" type="QString" name="border_width_map_unit_scale"/>
            <Option value="197,197,197,{update_symbology}" type="QString" name="color"/>
            <Option value="bevel" type="QString" name="joinstyle"/>
            <Option value="0,0" type="QString" name="offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="offset_unit"/>
            <Option value="35,35,35,255" type="QString" name="outline_color"/>
            <Option value="solid" type="QString" name="outline_style"/>
            <Option value="0.26" type="QString" name="outline_width"/>
            <Option value="MM" type="QString" name="outline_width_unit"/>
            <Option value="solid" type="QString" name="style"/>
          </Option>
          <prop v="3x:0,0,0,0,0,0" k="border_width_map_unit_scale"/>
          <prop v="197,197,197,{update_symbology}" k="color"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="0,0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="35,35,35,{update_symbology}" k="outline_color"/>
          <prop v="solid" k="outline_style"/>
          <prop v="0.26" k="outline_width"/>
          <prop v="MM" k="outline_width_unit"/>
          <prop v="solid" k="style"/>
          <data_defined_properties>
            <Option type="Map">
              <Option value="" type="QString" name="name"/>
              <Option name="properties"/>
              <Option value="collection" type="QString" name="type"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
    </symbols>
    <source-symbol>
      <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="fill" name="0">
        <data_defined_properties>
          <Option type="Map">
            <Option value="" type="QString" name="name"/>
            <Option name="properties"/>
            <Option value="collection" type="QString" name="type"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleFill" pass="0" enabled="1" locked="0">
          <Option type="Map">
            <Option value="3x:0,0,0,0,0,0" type="QString" name="border_width_map_unit_scale"/>
            <Option value="141,90,153,255" type="QString" name="color"/>
            <Option value="bevel" type="QString" name="joinstyle"/>
            <Option value="0,0" type="QString" name="offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="offset_unit"/>
            <Option value="35,35,35,255" type="QString" name="outline_color"/>
            <Option value="solid" type="QString" name="outline_style"/>
            <Option value="0.26" type="QString" name="outline_width"/>
            <Option value="MM" type="QString" name="outline_width_unit"/>
            <Option value="solid" type="QString" name="style"/>
          </Option>
          <prop v="3x:0,0,0,0,0,0" k="border_width_map_unit_scale"/>
          <prop v="141,90,153,255" k="color"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="0,0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="35,35,35,255" k="outline_color"/>
          <prop v="solid" k="outline_style"/>
          <prop v="0.26" k="outline_width"/>
          <prop v="MM" k="outline_width_unit"/>
          <prop v="solid" k="style"/>
          <data_defined_properties>
            <Option type="Map">
              <Option value="" type="QString" name="name"/>
              <Option name="properties"/>
              <Option value="collection" type="QString" name="type"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
    </source-symbol>
    <rotation/>
    <sizescale/>
  </renderer-v2>
  <blendMode>0</blendMode>
  <featureBlendMode>0</featureBlendMode>
  <layerGeometryType>2</layerGeometryType>
</qgis>
"""

STYLING_POLYGONS_DAMO = f"""
<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.22.4-Białowieża" styleCategories="Symbology">
  <renderer-v2 symbollevels="0" forceraster="0" enableorderby="0" type="categorizedSymbol" attr="in_both" referencescale="-1">
    <categories>
      <category symbol="0" render="true" label="{model_name} new {date_fn_damo_new}" value="{model_name} new"/>
      <category symbol="1" render="true" label="{model_name} old {date_fn_damo_old}" value="{model_name} old"/>
      <category symbol="2" render="true" label="{model_name} both" value="{model_name} both"/>
    </categories>
    <symbols>
      <symbol name="0" is_animated="0" type="fill" frame_rate="10" alpha="1" force_rhr="0" clip_to_extent="1">
        <data_defined_properties>
        <Option type="Map">
          <Option name="name" value="" type="QString"/>
          <Option name="properties"/>
          <Option name="type" value="collection" type="QString"/>
        </Option>
        </data_defined_properties>
        <layer pass="0" enabled="1" class="SimpleFill" locked="0">
        <Option type="Map">
          <Option name="border_width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
          <Option name="color" value="255,127,0,255" type="QString"/>
          <Option name="joinstyle" value="bevel" type="QString"/>
          <Option name="offset" value="0,0" type="QString"/>
          <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
          <Option name="offset_unit" value="MM" type="QString"/>
          <Option name="outline_color" value="35,35,35,255" type="QString"/>
          <Option name="outline_style" value="solid" type="QString"/>
          <Option name="outline_width" value="0.26" type="QString"/>
          <Option name="outline_width_unit" value="MM" type="QString"/>
          <Option name="style" value="solid" type="QString"/>
        </Option>
        <data_defined_properties>
          <Option type="Map">
          <Option name="name" value="" type="QString"/>
          <Option name="properties"/>
          <Option name="type" value="collection" type="QString"/>
          </Option>
        </data_defined_properties>
        </layer>
      </symbol>
      <symbol name="1" is_animated="0" type="fill" frame_rate="10" alpha="1" force_rhr="0" clip_to_extent="1">
        <data_defined_properties>
        <Option type="Map">
          <Option name="name" value="" type="QString"/>
          <Option name="properties"/>
          <Option name="type" value="collection" type="QString"/>
        </Option>
        </data_defined_properties>
        <layer pass="0" enabled="1" class="SimpleFill" locked="0">
        <Option type="Map">
          <Option name="border_width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
          <Option name="color" value="62,131,249,255" type="QString"/>
          <Option name="joinstyle" value="bevel" type="QString"/>
          <Option name="offset" value="0,0" type="QString"/>
          <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
          <Option name="offset_unit" value="MM" type="QString"/>
          <Option name="outline_color" value="35,35,35,255" type="QString"/>
          <Option name="outline_style" value="solid" type="QString"/>
          <Option name="outline_width" value="0.26" type="QString"/>
          <Option name="outline_width_unit" value="MM" type="QString"/>
          <Option name="style" value="solid" type="QString"/>
        </Option>
        <data_defined_properties>
          <Option type="Map">
          <Option name="name" value="" type="QString"/>
          <Option name="properties"/>
          <Option name="type" value="collection" type="QString"/>
          </Option>
        </data_defined_properties>
        </layer>
      </symbol>
      <symbol name="2" is_animated="0" type="fill" frame_rate="10" alpha="1" force_rhr="0" clip_to_extent="1">
        <data_defined_properties>
        <Option type="Map">
          <Option name="name" value="" type="QString"/>
          <Option name="properties"/>
          <Option name="type" value="collection" type="QString"/>
        </Option>
        </data_defined_properties>
        <layer pass="0" enabled="1" class="SimpleFill" locked="0">
        <Option type="Map">
          <Option name="border_width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
          <Option name="color" value="250,3,3,255" type="QString"/>
          <Option name="joinstyle" value="bevel" type="QString"/>
          <Option name="offset" value="0,0" type="QString"/>
          <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
          <Option name="offset_unit" value="MM" type="QString"/>
          <Option name="outline_color" value="250,3,3,0" type="QString"/>
          <Option name="outline_style" value="solid" type="QString"/>
          <Option name="outline_width" value="0" type="QString"/>
          <Option name="outline_width_unit" value="MM" type="QString"/>
          <Option name="style" value="solid" type="QString"/>
        </Option>
        <data_defined_properties>
          <Option type="Map">
          <Option name="name" value="" type="QString"/>
          <Option name="properties" type="Map">
            <Option name="fillStyle" type="Map">
            <Option name="active" value="true" type="bool"/>
            <Option name="expression" value="if(&quot;number_of_critical&quot;=0,'no','solid')" type="QString"/>
            <Option name="type" value="3" type="int"/>
            </Option>
            <Option name="outlineWidth" type="Map">
            <Option name="active" value="true" type="bool"/>
            <Option name="field" value="number_of_critical" type="QString"/>
            <Option name="type" value="2" type="int"/>
            </Option>
          </Option>
          <Option name="type" value="collection" type="QString"/>
          </Option>
        </data_defined_properties>
        </layer>
        <layer pass="0" enabled="1" class="SimpleFill" locked="0">
        <Option type="Map">
          <Option name="border_width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
          <Option name="color" value="197,197,197,{update_symbology}" type="QString"/>
          <Option name="joinstyle" value="bevel" type="QString"/>
          <Option name="offset" value="0,0" type="QString"/>
          <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
          <Option name="offset_unit" value="MM" type="QString"/>
          <Option name="outline_color" value="35,35,35,{update_symbology}" type="QString"/>
          <Option name="outline_style" value="solid" type="QString"/>
          <Option name="outline_width" value="0.26" type="QString"/>
          <Option name="outline_width_unit" value="MM" type="QString"/>
          <Option name="style" value="solid" type="QString"/>
        </Option>
        <data_defined_properties>
          <Option type="Map">
          <Option name="name" value="" type="QString"/>
          <Option name="properties"/>
          <Option name="type" value="collection" type="QString"/>
          </Option>
        </data_defined_properties>
        </layer>
      </symbol>
      </symbols>
      <source-symbol>
      <symbol name="0" is_animated="0" type="fill" frame_rate="10" alpha="1" force_rhr="0" clip_to_extent="1">
        <data_defined_properties>
        <Option type="Map">
          <Option name="name" value="" type="QString"/>
          <Option name="properties"/>
          <Option name="type" value="collection" type="QString"/>
        </Option>
        </data_defined_properties>
        <layer pass="0" enabled="1" class="SimpleFill" locked="0">
        <Option type="Map">
          <Option name="border_width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
          <Option name="color" value="141,90,153,255" type="QString"/>
          <Option name="joinstyle" value="bevel" type="QString"/>
          <Option name="offset" value="0,0" type="QString"/>
          <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
          <Option name="offset_unit" value="MM" type="QString"/>
          <Option name="outline_color" value="35,35,35,{update_symbology}" type="QString"/>
          <Option name="outline_style" value="solid" type="QString"/>
          <Option name="outline_width" value="0.26" type="QString"/>
          <Option name="outline_width_unit" value="MM" type="QString"/>
          <Option name="style" value="solid" type="QString"/>
        </Option>
        <data_defined_properties>
          <Option type="Map">
          <Option name="name" value="" type="QString"/>
          <Option name="properties"/>
          <Option name="type" value="collection" type="QString"/>
          </Option>
        </data_defined_properties>
        </layer>
      </symbol>
      </source-symbol>
      <rotation/>
      <sizescale/>
    </renderer-v2>
    <selection mode="Default">
      <selectionColor invalid="1"/>
    </selection>
    <blendMode>0</blendMode>
    <featureBlendMode>0</featureBlendMode>
    <layerGeometryType>2</layerGeometryType>
    </qgis>"""


STYLING_POINTS_THREEDI = f"""<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.22.4-Białowieża" styleCategories="Symbology">
  <renderer-v2 symbollevels="0" forceraster="0" enableorderby="0" type="categorizedSymbol" attr="in_both" referencescale="-1">
    <categories>
      <category symbol="0" render="true" label="Damo {model_name} {date_fn_damo_new}" value="{model_name} damo"/>
      <category symbol="1" render="true" label="Model {model_name} {date_threedi}" value="{model_name} sqlite"/>
      <category symbol="2" render="true" label="{model_name} both" value="{model_name} both"/>
    </categories>
    <symbols>
      <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="marker" name="0">
        <data_defined_properties>
          <Option type="Map">
            <Option value="" type="QString" name="name"/>
            <Option name="properties"/>
            <Option value="collection" type="QString" name="type"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleMarker" pass="0" enabled="1" locked="0">
          <Option type="Map">
            <Option value="0" type="QString" name="angle"/>
            <Option value="square" type="QString" name="cap_style"/>
            <Option value="255,127,0,255" type="QString" name="color"/>
            <Option value="1" type="QString" name="horizontal_anchor_point"/>
            <Option value="bevel" type="QString" name="joinstyle"/>
            <Option value="circle" type="QString" name="name"/>
            <Option value="0,0" type="QString" name="offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="offset_unit"/>
            <Option value="35,35,35,{update_symbology}" type="QString" name="outline_color"/>
            <Option value="solid" type="QString" name="outline_style"/>
            <Option value="0" type="QString" name="outline_width"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="outline_width_map_unit_scale"/>
            <Option value="MM" type="QString" name="outline_width_unit"/>
            <Option value="diameter" type="QString" name="scale_method"/>
            <Option value="2" type="QString" name="size"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="size_map_unit_scale"/>
            <Option value="MM" type="QString" name="size_unit"/>
            <Option value="1" type="QString" name="vertical_anchor_point"/>
          </Option>
          <prop v="0" k="angle"/>
          <prop v="square" k="cap_style"/>
          <prop v="255,127,0,255" k="color"/>
          <prop v="1" k="horizontal_anchor_point"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="circle" k="name"/>
          <prop v="0,0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="35,35,35,{update_symbology}" k="outline_color"/>
          <prop v="solid" k="outline_style"/>
          <prop v="0" k="outline_width"/>
          <prop v="3x:0,0,0,0,0,0" k="outline_width_map_unit_scale"/>
          <prop v="MM" k="outline_width_unit"/>
          <prop v="diameter" k="scale_method"/>
          <prop v="2" k="size"/>
          <prop v="3x:0,0,0,0,0,0" k="size_map_unit_scale"/>
          <prop v="MM" k="size_unit"/>
          <prop v="1" k="vertical_anchor_point"/>
          <data_defined_properties>
            <Option type="Map">
              <Option value="" type="QString" name="name"/>
              <Option name="properties"/>
              <Option value="collection" type="QString" name="type"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="marker" name="1">
        <data_defined_properties>
          <Option type="Map">
            <Option value="" type="QString" name="name"/>
            <Option name="properties"/>
            <Option value="collection" type="QString" name="type"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleMarker" pass="0" enabled="1" locked="0">
          <Option type="Map">
            <Option value="0" type="QString" name="angle"/>
            <Option value="square" type="QString" name="cap_style"/>
            <Option value="62,131,249,255" type="QString" name="color"/>
            <Option value="1" type="QString" name="horizontal_anchor_point"/>
            <Option value="bevel" type="QString" name="joinstyle"/>
            <Option value="circle" type="QString" name="name"/>
            <Option value="0,0" type="QString" name="offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="offset_unit"/>
            <Option value="35,35,35,{update_symbology}" type="QString" name="outline_color"/>
            <Option value="solid" type="QString" name="outline_style"/>
            <Option value="0" type="QString" name="outline_width"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="outline_width_map_unit_scale"/>
            <Option value="MM" type="QString" name="outline_width_unit"/>
            <Option value="diameter" type="QString" name="scale_method"/>
            <Option value="2" type="QString" name="size"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="size_map_unit_scale"/>
            <Option value="MM" type="QString" name="size_unit"/>
            <Option value="1" type="QString" name="vertical_anchor_point"/>
          </Option>
          <prop v="0" k="angle"/>
          <prop v="square" k="cap_style"/>
          <prop v="62,131,249,255" k="color"/>
          <prop v="1" k="horizontal_anchor_point"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="circle" k="name"/>
          <prop v="0,0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="35,35,35,{update_symbology}" k="outline_color"/>
          <prop v="solid" k="outline_style"/>
          <prop v="0" k="outline_width"/>
          <prop v="3x:0,0,0,0,0,0" k="outline_width_map_unit_scale"/>
          <prop v="MM" k="outline_width_unit"/>
          <prop v="diameter" k="scale_method"/>
          <prop v="2" k="size"/>
          <prop v="3x:0,0,0,0,0,0" k="size_map_unit_scale"/>
          <prop v="MM" k="size_unit"/>
          <prop v="1" k="vertical_anchor_point"/>
          <data_defined_properties>
            <Option type="Map">
              <Option value="" type="QString" name="name"/>
              <Option name="properties"/>
              <Option value="collection" type="QString" name="type"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="marker" name="2">
        <data_defined_properties>
          <Option type="Map">
            <Option value="" type="QString" name="name"/>
            <Option name="properties"/>
            <Option value="collection" type="QString" name="type"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleMarker" pass="0" enabled="1" locked="0">
          <Option type="Map">
            <Option value="0" type="QString" name="angle"/>
            <Option value="square" type="QString" name="cap_style"/>
            <Option value="197,197,197,{update_symbology}" type="QString" name="color"/>
            <Option value="1" type="QString" name="horizontal_anchor_point"/>
            <Option value="bevel" type="QString" name="joinstyle"/>
            <Option value="circle" type="QString" name="name"/>
            <Option value="0,0" type="QString" name="offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="offset_unit"/>
            <Option value="35,35,35,{update_symbology}" type="QString" name="outline_color"/>
            <Option value="solid" type="QString" name="outline_style"/>
            <Option value="0" type="QString" name="outline_width"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="outline_width_map_unit_scale"/>
            <Option value="MM" type="QString" name="outline_width_unit"/>
            <Option value="diameter" type="QString" name="scale_method"/>
            <Option value="2" type="QString" name="size"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="size_map_unit_scale"/>
            <Option value="MM" type="QString" name="size_unit"/>
            <Option value="1" type="QString" name="vertical_anchor_point"/>
          </Option>
          <prop v="0" k="angle"/>
          <prop v="square" k="cap_style"/>
          <prop v="197,197,197,{update_symbology}" k="color"/>
          <prop v="1" k="horizontal_anchor_point"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="circle" k="name"/>
          <prop v="0,0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="35,35,35,{update_symbology}" k="outline_color"/>
          <prop v="solid" k="outline_style"/>
          <prop v="0" k="outline_width"/>
          <prop v="3x:0,0,0,0,0,0" k="outline_width_map_unit_scale"/>
          <prop v="MM" k="outline_width_unit"/>
          <prop v="diameter" k="scale_method"/>
          <prop v="2" k="size"/>
          <prop v="3x:0,0,0,0,0,0" k="size_map_unit_scale"/>
          <prop v="MM" k="size_unit"/>
          <prop v="1" k="vertical_anchor_point"/>
          <data_defined_properties>
            <Option type="Map">
              <Option value="" type="QString" name="name"/>
              <Option name="properties"/>
              <Option value="collection" type="QString" name="type"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
    </symbols>
    <source-symbol>
      <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="marker" name="0">
        <data_defined_properties>
          <Option type="Map">
            <Option value="" type="QString" name="name"/>
            <Option name="properties"/>
            <Option value="collection" type="QString" name="type"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleMarker" pass="0" enabled="1" locked="0">
          <Option type="Map">
            <Option value="0" type="QString" name="angle"/>
            <Option value="square" type="QString" name="cap_style"/>
            <Option value="164,113,88,255" type="QString" name="color"/>
            <Option value="1" type="QString" name="horizontal_anchor_point"/>
            <Option value="bevel" type="QString" name="joinstyle"/>
            <Option value="circle" type="QString" name="name"/>
            <Option value="0,0" type="QString" name="offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="offset_unit"/>
            <Option value="35,35,35,{update_symbology}" type="QString" name="outline_color"/>
            <Option value="solid" type="QString" name="outline_style"/>
            <Option value="0" type="QString" name="outline_width"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="outline_width_map_unit_scale"/>
            <Option value="MM" type="QString" name="outline_width_unit"/>
            <Option value="diameter" type="QString" name="scale_method"/>
            <Option value="2" type="QString" name="size"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="size_map_unit_scale"/>
            <Option value="MM" type="QString" name="size_unit"/>
            <Option value="1" type="QString" name="vertical_anchor_point"/>
          </Option>
          <prop v="0" k="angle"/>
          <prop v="square" k="cap_style"/>
          <prop v="164,113,88,255" k="color"/>
          <prop v="1" k="horizontal_anchor_point"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="circle" k="name"/>
          <prop v="0,0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="35,35,35,255" k="outline_color"/>
          <prop v="solid" k="outline_style"/>
          <prop v="0" k="outline_width"/>
          <prop v="3x:0,0,0,0,0,0" k="outline_width_map_unit_scale"/>
          <prop v="MM" k="outline_width_unit"/>
          <prop v="diameter" k="scale_method"/>
          <prop v="2" k="size"/>
          <prop v="3x:0,0,0,0,0,0" k="size_map_unit_scale"/>
          <prop v="MM" k="size_unit"/>
          <prop v="1" k="vertical_anchor_point"/>
          <data_defined_properties>
            <Option type="Map">
              <Option value="" type="QString" name="name"/>
              <Option name="properties"/>
              <Option value="collection" type="QString" name="type"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
    </source-symbol>
    <rotation/>
    <sizescale/>
  </renderer-v2>
  <blendMode>0</blendMode>
  <featureBlendMode>0</featureBlendMode>
  <layerGeometryType>0</layerGeometryType>
</qgis>"""

STYLING_LINES_THREEDI = f"""<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.22.4-Białowieża" styleCategories="Symbology">
  <renderer-v2 symbollevels="0" forceraster="0" enableorderby="0" type="categorizedSymbol" attr="in_both" referencescale="-1">
    <categories>
      <category symbol="0" render="true" label="Damo {model_name} {date_fn_damo_new}" value="{model_name} damo"/>
      <category symbol="1" render="true" label="Model {model_name} {date_threedi}" value="{model_name} sqlite"/>
      <category symbol="2" render="true" label="{model_name} both" value="{model_name} both"/>
    </categories>
    <symbols>
      <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="line" name="0">
        <data_defined_properties>
          <Option type="Map">
            <Option value="" type="QString" name="name"/>
            <Option name="properties"/>
            <Option value="collection" type="QString" name="type"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleLine" pass="0" enabled="1" locked="0">
          <Option type="Map">
            <Option value="0" type="QString" name="align_dash_pattern"/>
            <Option value="square" type="QString" name="capstyle"/>
            <Option value="5;2" type="QString" name="customdash"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="customdash_map_unit_scale"/>
            <Option value="MM" type="QString" name="customdash_unit"/>
            <Option value="0" type="QString" name="dash_pattern_offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="dash_pattern_offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="dash_pattern_offset_unit"/>
            <Option value="0" type="QString" name="draw_inside_polygon"/>
            <Option value="bevel" type="QString" name="joinstyle"/>
            <Option value="255,127,0,255" type="QString" name="line_color"/>
            <Option value="solid" type="QString" name="line_style"/>
            <Option value="1" type="QString" name="line_width"/>
            <Option value="MM" type="QString" name="line_width_unit"/>
            <Option value="0" type="QString" name="offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="offset_unit"/>
            <Option value="0" type="QString" name="ring_filter"/>
            <Option value="0" type="QString" name="trim_distance_end"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="trim_distance_end_map_unit_scale"/>
            <Option value="MM" type="QString" name="trim_distance_end_unit"/>
            <Option value="0" type="QString" name="trim_distance_start"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="trim_distance_start_map_unit_scale"/>
            <Option value="MM" type="QString" name="trim_distance_start_unit"/>
            <Option value="0" type="QString" name="tweak_dash_pattern_on_corners"/>
            <Option value="0" type="QString" name="use_custom_dash"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="width_map_unit_scale"/>
          </Option>
          <prop v="0" k="align_dash_pattern"/>
          <prop v="square" k="capstyle"/>
          <prop v="5;2" k="customdash"/>
          <prop v="3x:0,0,0,0,0,0" k="customdash_map_unit_scale"/>
          <prop v="MM" k="customdash_unit"/>
          <prop v="0" k="dash_pattern_offset"/>
          <prop v="3x:0,0,0,0,0,0" k="dash_pattern_offset_map_unit_scale"/>
          <prop v="MM" k="dash_pattern_offset_unit"/>
          <prop v="0" k="draw_inside_polygon"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="255,127,0,255" k="line_color"/>
          <prop v="solid" k="line_style"/>
          <prop v="1" k="line_width"/>
          <prop v="MM" k="line_width_unit"/>
          <prop v="0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="0" k="ring_filter"/>
          <prop v="0" k="trim_distance_end"/>
          <prop v="3x:0,0,0,0,0,0" k="trim_distance_end_map_unit_scale"/>
          <prop v="MM" k="trim_distance_end_unit"/>
          <prop v="0" k="trim_distance_start"/>
          <prop v="3x:0,0,0,0,0,0" k="trim_distance_start_map_unit_scale"/>
          <prop v="MM" k="trim_distance_start_unit"/>
          <prop v="0" k="tweak_dash_pattern_on_corners"/>
          <prop v="0" k="use_custom_dash"/>
          <prop v="3x:0,0,0,0,0,0" k="width_map_unit_scale"/>
          <data_defined_properties>
            <Option type="Map">
              <Option value="" type="QString" name="name"/>
              <Option name="properties"/>
              <Option value="collection" type="QString" name="type"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="line" name="1">
        <data_defined_properties>
          <Option type="Map">
            <Option value="" type="QString" name="name"/>
            <Option name="properties"/>
            <Option value="collection" type="QString" name="type"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleLine" pass="0" enabled="1" locked="0">
          <Option type="Map">
            <Option value="0" type="QString" name="align_dash_pattern"/>
            <Option value="square" type="QString" name="capstyle"/>
            <Option value="5;2" type="QString" name="customdash"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="customdash_map_unit_scale"/>
            <Option value="MM" type="QString" name="customdash_unit"/>
            <Option value="0" type="QString" name="dash_pattern_offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="dash_pattern_offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="dash_pattern_offset_unit"/>
            <Option value="0" type="QString" name="draw_inside_polygon"/>
            <Option value="bevel" type="QString" name="joinstyle"/>
            <Option value="62,131,249,255" type="QString" name="line_color"/>
            <Option value="solid" type="QString" name="line_style"/>
            <Option value="1" type="QString" name="line_width"/>
            <Option value="MM" type="QString" name="line_width_unit"/>
            <Option value="0" type="QString" name="offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="offset_unit"/>
            <Option value="0" type="QString" name="ring_filter"/>
            <Option value="0" type="QString" name="trim_distance_end"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="trim_distance_end_map_unit_scale"/>
            <Option value="MM" type="QString" name="trim_distance_end_unit"/>
            <Option value="0" type="QString" name="trim_distance_start"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="trim_distance_start_map_unit_scale"/>
            <Option value="MM" type="QString" name="trim_distance_start_unit"/>
            <Option value="0" type="QString" name="tweak_dash_pattern_on_corners"/>
            <Option value="0" type="QString" name="use_custom_dash"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="width_map_unit_scale"/>
          </Option>
          <prop v="0" k="align_dash_pattern"/>
          <prop v="square" k="capstyle"/>
          <prop v="5;2" k="customdash"/>
          <prop v="3x:0,0,0,0,0,0" k="customdash_map_unit_scale"/>
          <prop v="MM" k="customdash_unit"/>
          <prop v="0" k="dash_pattern_offset"/>
          <prop v="3x:0,0,0,0,0,0" k="dash_pattern_offset_map_unit_scale"/>
          <prop v="MM" k="dash_pattern_offset_unit"/>
          <prop v="0" k="draw_inside_polygon"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="62,131,249,255" k="line_color"/>
          <prop v="solid" k="line_style"/>
          <prop v="1" k="line_width"/>
          <prop v="MM" k="line_width_unit"/>
          <prop v="0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="0" k="ring_filter"/>
          <prop v="0" k="trim_distance_end"/>
          <prop v="3x:0,0,0,0,0,0" k="trim_distance_end_map_unit_scale"/>
          <prop v="MM" k="trim_distance_end_unit"/>
          <prop v="0" k="trim_distance_start"/>
          <prop v="3x:0,0,0,0,0,0" k="trim_distance_start_map_unit_scale"/>
          <prop v="MM" k="trim_distance_start_unit"/>
          <prop v="0" k="tweak_dash_pattern_on_corners"/>
          <prop v="0" k="use_custom_dash"/>
          <prop v="3x:0,0,0,0,0,0" k="width_map_unit_scale"/>
          <data_defined_properties>
            <Option type="Map">
              <Option value="" type="QString" name="name"/>
              <Option name="properties"/>
              <Option value="collection" type="QString" name="type"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="line" name="2">
        <data_defined_properties>
          <Option type="Map">
            <Option value="" type="QString" name="name"/>
            <Option name="properties"/>
            <Option value="collection" type="QString" name="type"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleLine" pass="0" enabled="1" locked="0">
          <Option type="Map">
            <Option value="0" type="QString" name="align_dash_pattern"/>
            <Option value="square" type="QString" name="capstyle"/>
            <Option value="5;2" type="QString" name="customdash"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="customdash_map_unit_scale"/>
            <Option value="MM" type="QString" name="customdash_unit"/>
            <Option value="0" type="QString" name="dash_pattern_offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="dash_pattern_offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="dash_pattern_offset_unit"/>
            <Option value="0" type="QString" name="draw_inside_polygon"/>
            <Option value="bevel" type="QString" name="joinstyle"/>
            <Option value="197,197,197,{update_symbology}" type="QString" name="line_color"/>
            <Option value="solid" type="QString" name="line_style"/>
            <Option value="1" type="QString" name="line_width"/>
            <Option value="MM" type="QString" name="line_width_unit"/>
            <Option value="0" type="QString" name="offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="offset_unit"/>
            <Option value="0" type="QString" name="ring_filter"/>
            <Option value="0" type="QString" name="trim_distance_end"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="trim_distance_end_map_unit_scale"/>
            <Option value="MM" type="QString" name="trim_distance_end_unit"/>
            <Option value="0" type="QString" name="trim_distance_start"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="trim_distance_start_map_unit_scale"/>
            <Option value="MM" type="QString" name="trim_distance_start_unit"/>
            <Option value="0" type="QString" name="tweak_dash_pattern_on_corners"/>
            <Option value="0" type="QString" name="use_custom_dash"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="width_map_unit_scale"/>
          </Option>
          <prop v="0" k="align_dash_pattern"/>
          <prop v="square" k="capstyle"/>
          <prop v="5;2" k="customdash"/>
          <prop v="3x:0,0,0,0,0,0" k="customdash_map_unit_scale"/>
          <prop v="MM" k="customdash_unit"/>
          <prop v="0" k="dash_pattern_offset"/>
          <prop v="3x:0,0,0,0,0,0" k="dash_pattern_offset_map_unit_scale"/>
          <prop v="MM" k="dash_pattern_offset_unit"/>
          <prop v="0" k="draw_inside_polygon"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="197,197,197,{update_symbology}" k="line_color"/>
          <prop v="solid" k="line_style"/>
          <prop v="1" k="line_width"/>
          <prop v="MM" k="line_width_unit"/>
          <prop v="0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="0" k="ring_filter"/>
          <prop v="0" k="trim_distance_end"/>
          <prop v="3x:0,0,0,0,0,0" k="trim_distance_end_map_unit_scale"/>
          <prop v="MM" k="trim_distance_end_unit"/>
          <prop v="0" k="trim_distance_start"/>
          <prop v="3x:0,0,0,0,0,0" k="trim_distance_start_map_unit_scale"/>
          <prop v="MM" k="trim_distance_start_unit"/>
          <prop v="0" k="tweak_dash_pattern_on_corners"/>
          <prop v="0" k="use_custom_dash"/>
          <prop v="3x:0,0,0,0,0,0" k="width_map_unit_scale"/>
          <data_defined_properties>
            <Option type="Map">
              <Option value="" type="QString" name="name"/>
              <Option name="properties"/>
              <Option value="collection" type="QString" name="type"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
    </symbols>
    <source-symbol>
      <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="line" name="0">
        <data_defined_properties>
          <Option type="Map">
            <Option value="" type="QString" name="name"/>
            <Option name="properties"/>
            <Option value="collection" type="QString" name="type"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleLine" pass="0" enabled="1" locked="0">
          <Option type="Map">
            <Option value="0" type="QString" name="align_dash_pattern"/>
            <Option value="square" type="QString" name="capstyle"/>
            <Option value="5;2" type="QString" name="customdash"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="customdash_map_unit_scale"/>
            <Option value="MM" type="QString" name="customdash_unit"/>
            <Option value="0" type="QString" name="dash_pattern_offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="dash_pattern_offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="dash_pattern_offset_unit"/>
            <Option value="0" type="QString" name="draw_inside_polygon"/>
            <Option value="bevel" type="QString" name="joinstyle"/>
            <Option value="213,180,60,255" type="QString" name="line_color"/>
            <Option value="solid" type="QString" name="line_style"/>
            <Option value="1" type="QString" name="line_width"/>
            <Option value="MM" type="QString" name="line_width_unit"/>
            <Option value="0" type="QString" name="offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="offset_unit"/>
            <Option value="0" type="QString" name="ring_filter"/>
            <Option value="0" type="QString" name="trim_distance_end"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="trim_distance_end_map_unit_scale"/>
            <Option value="MM" type="QString" name="trim_distance_end_unit"/>
            <Option value="0" type="QString" name="trim_distance_start"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="trim_distance_start_map_unit_scale"/>
            <Option value="MM" type="QString" name="trim_distance_start_unit"/>
            <Option value="0" type="QString" name="tweak_dash_pattern_on_corners"/>
            <Option value="0" type="QString" name="use_custom_dash"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="width_map_unit_scale"/>
          </Option>
          <prop v="0" k="align_dash_pattern"/>
          <prop v="square" k="capstyle"/>
          <prop v="5;2" k="customdash"/>
          <prop v="3x:0,0,0,0,0,0" k="customdash_map_unit_scale"/>
          <prop v="MM" k="customdash_unit"/>
          <prop v="0" k="dash_pattern_offset"/>
          <prop v="3x:0,0,0,0,0,0" k="dash_pattern_offset_map_unit_scale"/>
          <prop v="MM" k="dash_pattern_offset_unit"/>
          <prop v="0" k="draw_inside_polygon"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="213,180,60,255" k="line_color"/>
          <prop v="solid" k="line_style"/>
          <prop v="1" k="line_width"/>
          <prop v="MM" k="line_width_unit"/>
          <prop v="0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="0" k="ring_filter"/>
          <prop v="0" k="trim_distance_end"/>
          <prop v="3x:0,0,0,0,0,0" k="trim_distance_end_map_unit_scale"/>
          <prop v="MM" k="trim_distance_end_unit"/>
          <prop v="0" k="trim_distance_start"/>
          <prop v="3x:0,0,0,0,0,0" k="trim_distance_start_map_unit_scale"/>
          <prop v="MM" k="trim_distance_start_unit"/>
          <prop v="0" k="tweak_dash_pattern_on_corners"/>
          <prop v="0" k="use_custom_dash"/>
          <prop v="3x:0,0,0,0,0,0" k="width_map_unit_scale"/>
          <data_defined_properties>
            <Option type="Map">
              <Option value="" type="QString" name="name"/>
              <Option name="properties"/>
              <Option value="collection" type="QString" name="type"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
    </source-symbol>
    <rotation/>
    <sizescale/>
  </renderer-v2>
  <blendMode>0</blendMode>
  <featureBlendMode>0</featureBlendMode>
  <layerGeometryType>1</layerGeometryType>
</qgis>"""

STYLING_POLYGONS_THREEDI = f"""
<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.22.4-Białowieża" styleCategories="Symbology">
  <renderer-v2 symbollevels="0" forceraster="0" enableorderby="0" type="categorizedSymbol" attr="in_both" referencescale="-1">
    <categories>
      <category symbol="0" render="true" label="Damo {model_name} {date_fn_damo_new}" value="{model_name} sqlite"/>
      <category symbol="1" render="true" label="Model {model_name} {date_threedi}" value="{model_name} damo"/>
      <category symbol="2" render="true" label="{model_name} both" value="{model_name} both"/>
    </categories>
    <symbols>
      <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="fill" name="0">
        <data_defined_properties>
          <Option type="Map">
            <Option value="" type="QString" name="name"/>
            <Option name="properties"/>
            <Option value="collection" type="QString" name="type"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleFill" pass="0" enabled="1" locked="0">
          <Option type="Map">
            <Option value="3x:0,0,0,0,0,0" type="QString" name="border_width_map_unit_scale"/>
            <Option value="255,127,0,255" type="QString" name="color"/>
            <Option value="bevel" type="QString" name="joinstyle"/>
            <Option value="0,0" type="QString" name="offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="offset_unit"/>
            <Option value="35,35,35,255" type="QString" name="outline_color"/>
            <Option value="solid" type="QString" name="outline_style"/>
            <Option value="0.26" type="QString" name="outline_width"/>
            <Option value="MM" type="QString" name="outline_width_unit"/>
            <Option value="solid" type="QString" name="style"/>
          </Option>
          <prop v="3x:0,0,0,0,0,0" k="border_width_map_unit_scale"/>
          <prop v="255,127,0,255" k="color"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="0,0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="35,35,35,255" k="outline_color"/>
          <prop v="solid" k="outline_style"/>
          <prop v="0.26" k="outline_width"/>
          <prop v="MM" k="outline_width_unit"/>
          <prop v="solid" k="style"/>
          <data_defined_properties>
            <Option type="Map">
              <Option value="" type="QString" name="name"/>
              <Option name="properties"/>
              <Option value="collection" type="QString" name="type"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="fill" name="1">
        <data_defined_properties>
          <Option type="Map">
            <Option value="" type="QString" name="name"/>
            <Option name="properties"/>
            <Option value="collection" type="QString" name="type"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleFill" pass="0" enabled="1" locked="0">
          <Option type="Map">
            <Option value="3x:0,0,0,0,0,0" type="QString" name="border_width_map_unit_scale"/>
            <Option value="62,131,249,255" type="QString" name="color"/>
            <Option value="bevel" type="QString" name="joinstyle"/>
            <Option value="0,0" type="QString" name="offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="offset_unit"/>
            <Option value="35,35,35,255" type="QString" name="outline_color"/>
            <Option value="solid" type="QString" name="outline_style"/>
            <Option value="0.26" type="QString" name="outline_width"/>
            <Option value="MM" type="QString" name="outline_width_unit"/>
            <Option value="solid" type="QString" name="style"/>
          </Option>
          <prop v="3x:0,0,0,0,0,0" k="border_width_map_unit_scale"/>
          <prop v="62,131,249,255" k="color"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="0,0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="35,35,35,255" k="outline_color"/>
          <prop v="solid" k="outline_style"/>
          <prop v="0.26" k="outline_width"/>
          <prop v="MM" k="outline_width_unit"/>
          <prop v="solid" k="style"/>
          <data_defined_properties>
            <Option type="Map">
              <Option value="" type="QString" name="name"/>
              <Option name="properties"/>
              <Option value="collection" type="QString" name="type"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="fill" name="2">
        <data_defined_properties>
          <Option type="Map">
            <Option value="" type="QString" name="name"/>
            <Option name="properties"/>
            <Option value="collection" type="QString" name="type"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleFill" pass="0" enabled="1" locked="0">
          <Option type="Map">
            <Option value="3x:0,0,0,0,0,0" type="QString" name="border_width_map_unit_scale"/>
            <Option value="197,197,197,{update_symbology}" type="QString" name="color"/>
            <Option value="bevel" type="QString" name="joinstyle"/>
            <Option value="0,0" type="QString" name="offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="offset_unit"/>
            <Option value="35,35,35,255" type="QString" name="outline_color"/>
            <Option value="solid" type="QString" name="outline_style"/>
            <Option value="0.26" type="QString" name="outline_width"/>
            <Option value="MM" type="QString" name="outline_width_unit"/>
            <Option value="solid" type="QString" name="style"/>
          </Option>
          <prop v="3x:0,0,0,0,0,0" k="border_width_map_unit_scale"/>
          <prop v="197,197,197,{update_symbology}" k="color"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="0,0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="35,35,35,{update_symbology}" k="outline_color"/>
          <prop v="solid" k="outline_style"/>
          <prop v="0.26" k="outline_width"/>
          <prop v="MM" k="outline_width_unit"/>
          <prop v="solid" k="style"/>
          <data_defined_properties>
            <Option type="Map">
              <Option value="" type="QString" name="name"/>
              <Option name="properties"/>
              <Option value="collection" type="QString" name="type"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
    </symbols>
    <source-symbol>
      <symbol alpha="1" force_rhr="0" clip_to_extent="1" type="fill" name="0">
        <data_defined_properties>
          <Option type="Map">
            <Option value="" type="QString" name="name"/>
            <Option name="properties"/>
            <Option value="collection" type="QString" name="type"/>
          </Option>
        </data_defined_properties>
        <layer class="SimpleFill" pass="0" enabled="1" locked="0">
          <Option type="Map">
            <Option value="3x:0,0,0,0,0,0" type="QString" name="border_width_map_unit_scale"/>
            <Option value="141,90,153,255" type="QString" name="color"/>
            <Option value="bevel" type="QString" name="joinstyle"/>s
            <Option value="0,0" type="QString" name="offset"/>
            <Option value="3x:0,0,0,0,0,0" type="QString" name="offset_map_unit_scale"/>
            <Option value="MM" type="QString" name="offset_unit"/>
            <Option value="35,35,35,255" type="QString" name="outline_color"/>
            <Option value="solid" type="QString" name="outline_style"/>
            <Option value="0.26" type="QString" name="outline_width"/>
            <Option value="MM" type="QString" name="outline_width_unit"/>
            <Option value="solid" type="QString" name="style"/>
          </Option>
          <prop v="3x:0,0,0,0,0,0" k="border_width_map_unit_scale"/>
          <prop v="141,90,153,255" k="color"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="0,0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="35,35,35,255" k="outline_color"/>
          <prop v="solid" k="outline_style"/>
          <prop v="0.26" k="outline_width"/>
          <prop v="MM" k="outline_width_unit"/>
          <prop v="solid" k="style"/>
          <data_defined_properties>
            <Option type="Map">
              <Option value="" type="QString" name="name"/>
              <Option name="properties"/>
              <Option value="collection" type="QString" name="type"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
    </source-symbol>
    <rotation/>
    <sizescale/>
  </renderer-v2>
  <blendMode>0</blendMode>
  <featureBlendMode>0</featureBlendMode>
  <layerGeometryType>2</layerGeometryType>
</qgis>
"""

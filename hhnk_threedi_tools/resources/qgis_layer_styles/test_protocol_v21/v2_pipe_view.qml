<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis simplifyLocal="1" readOnly="0" labelsEnabled="0" styleCategories="AllStyleCategories" simplifyAlgorithm="0" hasScaleBasedVisibilityFlag="0" minScale="100000000" simplifyMaxScale="1" version="3.16.11-Hannover" maxScale="0" simplifyDrawingTol="1" simplifyDrawingHints="1">
  <flags>
    <Identifiable>1</Identifiable>
    <Removable>1</Removable>
    <Searchable>1</Searchable>
  </flags>
  <temporal fixedDuration="0" enabled="0" endField="" durationField="" accumulate="0" startField="" durationUnit="min" startExpression="" mode="0" endExpression="">
    <fixedRange>
      <start></start>
      <end></end>
    </fixedRange>
  </temporal>
  <renderer-v2 symbollevels="0" forceraster="0" type="RuleRenderer" enableorderby="0">
    <rules key="{1c4a4e03-d442-4bb0-8ffc-82b9a703e08f}">
      <rule symbol="0" key="{844e5e28-ad8f-43dc-ae9b-2eedeecde873}" label="Combined sewer" filter="pipe_sewerage_type = 0"/>
      <rule symbol="1" key="{3b41f70d-2dfe-4438-8b1a-3722a52ff82b}" label="Storm drain" filter="pipe_sewerage_type = 1"/>
      <rule symbol="2" key="{c8833167-878e-49b2-bd37-5019aeea2451}" label="Sanitary sewer" filter="pipe_sewerage_type = 2"/>
      <rule symbol="3" key="{d62bccfa-4138-43ba-ab6d-51eae9f5b079}" label="Transport" filter="pipe_sewerage_type = 3"/>
      <rule symbol="4" key="{3d909156-553e-4d45-8a2f-02337ffb74d5}" label="Spillway" filter="pipe_sewerage_type = 4"/>
      <rule symbol="5" key="{a445abaf-878b-4b6b-8f1d-1314d1271d38}" label="Syphon" filter="pipe_sewerage_type =5"/>
      <rule symbol="6" key="{c6ba261b-8172-407e-bc72-8487b24a1cc4}" label="Storage" filter="pipe_sewerage_type = 6"/>
      <rule symbol="7" key="{8eb66a66-0335-4672-a78d-1aac6c4702ff}" label="Storage and settlement tank" filter="pipe_sewerage_type = 7"/>
      <rule symbol="8" key="{aa320dac-96a8-41e4-af30-3e9153ceaeae}" label="Other" filter="ELSE"/>
    </rules>
    <symbols>
      <symbol type="line" name="0" alpha="1" clip_to_extent="1" force_rhr="0">
        <layer enabled="1" pass="0" class="SimpleLine" locked="0">
          <prop v="0" k="align_dash_pattern"/>
          <prop v="square" k="capstyle"/>
          <prop v="0" k="customdash"/>
          <prop v="3x:0,0,0,0,0,0" k="customdash_map_unit_scale"/>
          <prop v="MM" k="customdash_unit"/>
          <prop v="0" k="dash_pattern_offset"/>
          <prop v="3x:0,0,0,0,0,0" k="dash_pattern_offset_map_unit_scale"/>
          <prop v="MM" k="dash_pattern_offset_unit"/>
          <prop v="0" k="draw_inside_polygon"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="255,170,0,255" k="line_color"/>
          <prop v="solid" k="line_style"/>
          <prop v="0.4" k="line_width"/>
          <prop v="MM" k="line_width_unit"/>
          <prop v="0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="0" k="ring_filter"/>
          <prop v="0" k="tweak_dash_pattern_on_corners"/>
          <prop v="0" k="use_custom_dash"/>
          <prop v="3x:0,0,0,0,0,0" k="width_map_unit_scale"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol type="line" name="1" alpha="1" clip_to_extent="1" force_rhr="0">
        <layer enabled="1" pass="0" class="SimpleLine" locked="0">
          <prop v="0" k="align_dash_pattern"/>
          <prop v="square" k="capstyle"/>
          <prop v="0" k="customdash"/>
          <prop v="3x:0,0,0,0,0,0" k="customdash_map_unit_scale"/>
          <prop v="MM" k="customdash_unit"/>
          <prop v="0" k="dash_pattern_offset"/>
          <prop v="3x:0,0,0,0,0,0" k="dash_pattern_offset_map_unit_scale"/>
          <prop v="MM" k="dash_pattern_offset_unit"/>
          <prop v="0" k="draw_inside_polygon"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="85,170,255,255" k="line_color"/>
          <prop v="solid" k="line_style"/>
          <prop v="0.4" k="line_width"/>
          <prop v="MM" k="line_width_unit"/>
          <prop v="0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="0" k="ring_filter"/>
          <prop v="0" k="tweak_dash_pattern_on_corners"/>
          <prop v="0" k="use_custom_dash"/>
          <prop v="3x:0,0,0,0,0,0" k="width_map_unit_scale"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol type="line" name="2" alpha="1" clip_to_extent="1" force_rhr="0">
        <layer enabled="1" pass="0" class="SimpleLine" locked="0">
          <prop v="0" k="align_dash_pattern"/>
          <prop v="square" k="capstyle"/>
          <prop v="0" k="customdash"/>
          <prop v="3x:0,0,0,0,0,0" k="customdash_map_unit_scale"/>
          <prop v="MM" k="customdash_unit"/>
          <prop v="0" k="dash_pattern_offset"/>
          <prop v="3x:0,0,0,0,0,0" k="dash_pattern_offset_map_unit_scale"/>
          <prop v="MM" k="dash_pattern_offset_unit"/>
          <prop v="0" k="draw_inside_polygon"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="255,0,0,255" k="line_color"/>
          <prop v="solid" k="line_style"/>
          <prop v="0.4" k="line_width"/>
          <prop v="MM" k="line_width_unit"/>
          <prop v="0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="0" k="ring_filter"/>
          <prop v="0" k="tweak_dash_pattern_on_corners"/>
          <prop v="0" k="use_custom_dash"/>
          <prop v="3x:0,0,0,0,0,0" k="width_map_unit_scale"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol type="line" name="3" alpha="1" clip_to_extent="1" force_rhr="0">
        <layer enabled="1" pass="0" class="SimpleLine" locked="0">
          <prop v="0" k="align_dash_pattern"/>
          <prop v="square" k="capstyle"/>
          <prop v="0" k="customdash"/>
          <prop v="3x:0,0,0,0,0,0" k="customdash_map_unit_scale"/>
          <prop v="MM" k="customdash_unit"/>
          <prop v="0" k="dash_pattern_offset"/>
          <prop v="3x:0,0,0,0,0,0" k="dash_pattern_offset_map_unit_scale"/>
          <prop v="MM" k="dash_pattern_offset_unit"/>
          <prop v="0" k="draw_inside_polygon"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="153,153,153,255" k="line_color"/>
          <prop v="solid" k="line_style"/>
          <prop v="0.7" k="line_width"/>
          <prop v="MM" k="line_width_unit"/>
          <prop v="0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="0" k="ring_filter"/>
          <prop v="0" k="tweak_dash_pattern_on_corners"/>
          <prop v="0" k="use_custom_dash"/>
          <prop v="3x:0,0,0,0,0,0" k="width_map_unit_scale"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol type="line" name="4" alpha="1" clip_to_extent="1" force_rhr="0">
        <layer enabled="1" pass="0" class="SimpleLine" locked="0">
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
          <prop v="85,170,255,255" k="line_color"/>
          <prop v="dot" k="line_style"/>
          <prop v="0.4" k="line_width"/>
          <prop v="MM" k="line_width_unit"/>
          <prop v="0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="0" k="ring_filter"/>
          <prop v="0" k="tweak_dash_pattern_on_corners"/>
          <prop v="0" k="use_custom_dash"/>
          <prop v="3x:0,0,0,0,0,0" k="width_map_unit_scale"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol type="line" name="5" alpha="1" clip_to_extent="1" force_rhr="0">
        <layer enabled="1" pass="0" class="SimpleLine" locked="0">
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
          <prop v="85,170,255,255" k="line_color"/>
          <prop v="solid" k="line_style"/>
          <prop v="0.4" k="line_width"/>
          <prop v="MM" k="line_width_unit"/>
          <prop v="0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="0" k="ring_filter"/>
          <prop v="0" k="tweak_dash_pattern_on_corners"/>
          <prop v="0" k="use_custom_dash"/>
          <prop v="3x:0,0,0,0,0,0" k="width_map_unit_scale"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
        <layer enabled="1" pass="0" class="MarkerLine" locked="0">
          <prop v="4" k="average_angle_length"/>
          <prop v="3x:0,0,0,0,0,0" k="average_angle_map_unit_scale"/>
          <prop v="MM" k="average_angle_unit"/>
          <prop v="3" k="interval"/>
          <prop v="3x:0,0,0,0,0,0" k="interval_map_unit_scale"/>
          <prop v="MM" k="interval_unit"/>
          <prop v="0" k="offset"/>
          <prop v="0" k="offset_along_line"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_along_line_map_unit_scale"/>
          <prop v="MM" k="offset_along_line_unit"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="interval" k="placement"/>
          <prop v="0" k="ring_filter"/>
          <prop v="1" k="rotate"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
          <symbol type="marker" name="@5@1" alpha="1" clip_to_extent="1" force_rhr="0">
            <layer enabled="1" pass="0" class="SimpleMarker" locked="0">
              <prop v="0" k="angle"/>
              <prop v="85,170,255,255" k="color"/>
              <prop v="1" k="horizontal_anchor_point"/>
              <prop v="bevel" k="joinstyle"/>
              <prop v="semi_circle" k="name"/>
              <prop v="0,0" k="offset"/>
              <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
              <prop v="MM" k="offset_unit"/>
              <prop v="35,35,35,255" k="outline_color"/>
              <prop v="no" k="outline_style"/>
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
                  <Option type="QString" name="name" value=""/>
                  <Option name="properties"/>
                  <Option type="QString" name="type" value="collection"/>
                </Option>
              </data_defined_properties>
            </layer>
          </symbol>
        </layer>
      </symbol>
      <symbol type="line" name="6" alpha="1" clip_to_extent="1" force_rhr="0">
        <layer enabled="1" pass="0" class="SimpleLine" locked="0">
          <prop v="0" k="align_dash_pattern"/>
          <prop v="square" k="capstyle"/>
          <prop v="0" k="customdash"/>
          <prop v="3x:0,0,0,0,0,0" k="customdash_map_unit_scale"/>
          <prop v="MM" k="customdash_unit"/>
          <prop v="0" k="dash_pattern_offset"/>
          <prop v="3x:0,0,0,0,0,0" k="dash_pattern_offset_map_unit_scale"/>
          <prop v="MM" k="dash_pattern_offset_unit"/>
          <prop v="0" k="draw_inside_polygon"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="189,189,189,255" k="line_color"/>
          <prop v="solid" k="line_style"/>
          <prop v="2" k="line_width"/>
          <prop v="MM" k="line_width_unit"/>
          <prop v="0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="0" k="ring_filter"/>
          <prop v="0" k="tweak_dash_pattern_on_corners"/>
          <prop v="0" k="use_custom_dash"/>
          <prop v="3x:0,0,0,0,0,0" k="width_map_unit_scale"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol type="line" name="7" alpha="1" clip_to_extent="1" force_rhr="0">
        <layer enabled="1" pass="0" class="SimpleLine" locked="0">
          <prop v="0" k="align_dash_pattern"/>
          <prop v="square" k="capstyle"/>
          <prop v="0" k="customdash"/>
          <prop v="3x:0,0,0,0,0,0" k="customdash_map_unit_scale"/>
          <prop v="MM" k="customdash_unit"/>
          <prop v="0" k="dash_pattern_offset"/>
          <prop v="3x:0,0,0,0,0,0" k="dash_pattern_offset_map_unit_scale"/>
          <prop v="MM" k="dash_pattern_offset_unit"/>
          <prop v="0" k="draw_inside_polygon"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="92,92,92,255" k="line_color"/>
          <prop v="solid" k="line_style"/>
          <prop v="2" k="line_width"/>
          <prop v="MM" k="line_width_unit"/>
          <prop v="0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="0" k="ring_filter"/>
          <prop v="0" k="tweak_dash_pattern_on_corners"/>
          <prop v="0" k="use_custom_dash"/>
          <prop v="3x:0,0,0,0,0,0" k="width_map_unit_scale"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
      <symbol type="line" name="8" alpha="1" clip_to_extent="1" force_rhr="0">
        <layer enabled="1" pass="0" class="SimpleLine" locked="0">
          <prop v="0" k="align_dash_pattern"/>
          <prop v="square" k="capstyle"/>
          <prop v="0" k="customdash"/>
          <prop v="3x:0,0,0,0,0,0" k="customdash_map_unit_scale"/>
          <prop v="MM" k="customdash_unit"/>
          <prop v="0" k="dash_pattern_offset"/>
          <prop v="3x:0,0,0,0,0,0" k="dash_pattern_offset_map_unit_scale"/>
          <prop v="MM" k="dash_pattern_offset_unit"/>
          <prop v="0" k="draw_inside_polygon"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="0,0,0,255" k="line_color"/>
          <prop v="dot" k="line_style"/>
          <prop v="0.4" k="line_width"/>
          <prop v="MM" k="line_width_unit"/>
          <prop v="0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="0" k="ring_filter"/>
          <prop v="0" k="tweak_dash_pattern_on_corners"/>
          <prop v="0" k="use_custom_dash"/>
          <prop v="3x:0,0,0,0,0,0" k="width_map_unit_scale"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
    </symbols>
  </renderer-v2>
  <labeling type="simple">
    <settings calloutType="simple">
      <text-style fontLetterSpacing="0" fontWeight="50" useSubstitutions="0" blendMode="0" multilineHeight="1" previewBkgrdColor="255,255,255,255" isExpression="0" fontWordSpacing="0" fontStrikeout="0" textColor="0,0,0,255" fontSize="8.25" textOrientation="horizontal" fontKerning="1" fontFamily="MS Shell Dlg 2" allowHtml="0" fontItalic="0" fontSizeMapUnitScale="3x:0,0,0,0,0,0" capitalization="0" fontUnderline="0" fieldName="ROWID" fontSizeUnit="Point" textOpacity="1" namedStyle="Standaard">
        <text-buffer bufferOpacity="1" bufferColor="255,255,255,255" bufferSizeUnits="MM" bufferNoFill="0" bufferDraw="0" bufferSizeMapUnitScale="3x:0,0,0,0,0,0" bufferSize="1" bufferBlendMode="0" bufferJoinStyle="64"/>
        <text-mask maskSize="0" maskOpacity="1" maskType="0" maskEnabled="0" maskSizeMapUnitScale="3x:0,0,0,0,0,0" maskedSymbolLayers="" maskSizeUnits="MM" maskJoinStyle="128"/>
        <background shapeRadiiUnit="MM" shapeRadiiMapUnitScale="3x:0,0,0,0,0,0" shapeOffsetX="0" shapeOffsetY="0" shapeBorderColor="128,128,128,255" shapeSizeUnit="MM" shapeType="0" shapeOffsetMapUnitScale="3x:0,0,0,0,0,0" shapeRotation="0" shapeBorderWidthMapUnitScale="3x:0,0,0,0,0,0" shapeSizeType="0" shapeSizeMapUnitScale="3x:0,0,0,0,0,0" shapeRadiiX="0" shapeJoinStyle="64" shapeBlendMode="0" shapeBorderWidth="0" shapeDraw="0" shapeRotationType="0" shapeRadiiY="0" shapeBorderWidthUnit="MM" shapeSizeY="0" shapeOpacity="1" shapeOffsetUnit="MM" shapeSVGFile="" shapeSizeX="0" shapeFillColor="255,255,255,255">
          <symbol type="marker" name="markerSymbol" alpha="1" clip_to_extent="1" force_rhr="0">
            <layer enabled="1" pass="0" class="SimpleMarker" locked="0">
              <prop v="0" k="angle"/>
              <prop v="213,180,60,255" k="color"/>
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
                  <Option type="QString" name="name" value=""/>
                  <Option name="properties"/>
                  <Option type="QString" name="type" value="collection"/>
                </Option>
              </data_defined_properties>
            </layer>
          </symbol>
        </background>
        <shadow shadowDraw="0" shadowOffsetDist="1" shadowUnder="0" shadowOffsetMapUnitScale="3x:0,0,0,0,0,0" shadowOffsetUnit="MM" shadowScale="100" shadowOffsetGlobal="1" shadowRadiusUnit="MM" shadowColor="0,0,0,255" shadowOffsetAngle="135" shadowRadiusAlphaOnly="0" shadowOpacity="0.7" shadowRadiusMapUnitScale="3x:0,0,0,0,0,0" shadowBlendMode="6" shadowRadius="1.5"/>
        <dd_properties>
          <Option type="Map">
            <Option type="QString" name="name" value=""/>
            <Option name="properties"/>
            <Option type="QString" name="type" value="collection"/>
          </Option>
        </dd_properties>
        <substitutions/>
      </text-style>
      <text-format rightDirectionSymbol=">" reverseDirectionSymbol="0" placeDirectionSymbol="0" formatNumbers="0" addDirectionSymbol="0" wrapChar="" useMaxLineLengthForAutoWrap="1" leftDirectionSymbol="&lt;" plussign="0" decimals="3" autoWrapLength="0" multilineAlign="0"/>
      <placement placementFlags="10" overrunDistanceMapUnitScale="3x:0,0,0,0,0,0" offsetType="0" lineAnchorType="0" centroidInside="0" yOffset="0" repeatDistanceUnits="MM" fitInPolygonOnly="0" repeatDistanceMapUnitScale="3x:0,0,0,0,0,0" dist="0" polygonPlacementFlags="2" geometryGeneratorEnabled="0" layerType="LineGeometry" maxCurvedCharAngleIn="20" distUnits="MM" priority="5" lineAnchorPercent="0.5" overrunDistanceUnit="MM" distMapUnitScale="3x:0,0,0,0,0,0" centroidWhole="0" maxCurvedCharAngleOut="-20" rotationAngle="0" geometryGenerator="" offsetUnits="MapUnit" labelOffsetMapUnitScale="3x:0,0,0,0,0,0" predefinedPositionOrder="TR,TL,BR,BL,R,L,TSR,BSR" repeatDistance="0" xOffset="0" preserveRotation="1" overrunDistance="0" quadOffset="4" placement="2" geometryGeneratorType="PointGeometry"/>
      <rendering displayAll="0" minFeatureSize="0" labelPerPart="0" drawLabels="1" fontMaxPixelSize="10000" obstacleType="0" scaleVisibility="0" scaleMin="1" fontMinPixelSize="3" scaleMax="10000000" zIndex="0" upsidedownLabels="0" fontLimitPixelSize="0" maxNumLabels="2000" mergeLines="0" obstacle="1" limitNumLabels="0" obstacleFactor="1"/>
      <dd_properties>
        <Option type="Map">
          <Option type="QString" name="name" value=""/>
          <Option name="properties"/>
          <Option type="QString" name="type" value="collection"/>
        </Option>
      </dd_properties>
      <callout type="simple">
        <Option type="Map">
          <Option type="QString" name="anchorPoint" value="pole_of_inaccessibility"/>
          <Option type="Map" name="ddProperties">
            <Option type="QString" name="name" value=""/>
            <Option name="properties"/>
            <Option type="QString" name="type" value="collection"/>
          </Option>
          <Option type="bool" name="drawToAllParts" value="false"/>
          <Option type="QString" name="enabled" value="0"/>
          <Option type="QString" name="labelAnchorPoint" value="point_on_exterior"/>
          <Option type="QString" name="lineSymbol" value="&lt;symbol type=&quot;line&quot; name=&quot;symbol&quot; alpha=&quot;1&quot; clip_to_extent=&quot;1&quot; force_rhr=&quot;0&quot;>&lt;layer enabled=&quot;1&quot; pass=&quot;0&quot; class=&quot;SimpleLine&quot; locked=&quot;0&quot;>&lt;prop v=&quot;0&quot; k=&quot;align_dash_pattern&quot;/>&lt;prop v=&quot;square&quot; k=&quot;capstyle&quot;/>&lt;prop v=&quot;5;2&quot; k=&quot;customdash&quot;/>&lt;prop v=&quot;3x:0,0,0,0,0,0&quot; k=&quot;customdash_map_unit_scale&quot;/>&lt;prop v=&quot;MM&quot; k=&quot;customdash_unit&quot;/>&lt;prop v=&quot;0&quot; k=&quot;dash_pattern_offset&quot;/>&lt;prop v=&quot;3x:0,0,0,0,0,0&quot; k=&quot;dash_pattern_offset_map_unit_scale&quot;/>&lt;prop v=&quot;MM&quot; k=&quot;dash_pattern_offset_unit&quot;/>&lt;prop v=&quot;0&quot; k=&quot;draw_inside_polygon&quot;/>&lt;prop v=&quot;bevel&quot; k=&quot;joinstyle&quot;/>&lt;prop v=&quot;60,60,60,255&quot; k=&quot;line_color&quot;/>&lt;prop v=&quot;solid&quot; k=&quot;line_style&quot;/>&lt;prop v=&quot;0.3&quot; k=&quot;line_width&quot;/>&lt;prop v=&quot;MM&quot; k=&quot;line_width_unit&quot;/>&lt;prop v=&quot;0&quot; k=&quot;offset&quot;/>&lt;prop v=&quot;3x:0,0,0,0,0,0&quot; k=&quot;offset_map_unit_scale&quot;/>&lt;prop v=&quot;MM&quot; k=&quot;offset_unit&quot;/>&lt;prop v=&quot;0&quot; k=&quot;ring_filter&quot;/>&lt;prop v=&quot;0&quot; k=&quot;tweak_dash_pattern_on_corners&quot;/>&lt;prop v=&quot;0&quot; k=&quot;use_custom_dash&quot;/>&lt;prop v=&quot;3x:0,0,0,0,0,0&quot; k=&quot;width_map_unit_scale&quot;/>&lt;data_defined_properties>&lt;Option type=&quot;Map&quot;>&lt;Option type=&quot;QString&quot; name=&quot;name&quot; value=&quot;&quot;/>&lt;Option name=&quot;properties&quot;/>&lt;Option type=&quot;QString&quot; name=&quot;type&quot; value=&quot;collection&quot;/>&lt;/Option>&lt;/data_defined_properties>&lt;/layer>&lt;/symbol>"/>
          <Option type="double" name="minLength" value="0"/>
          <Option type="QString" name="minLengthMapUnitScale" value="3x:0,0,0,0,0,0"/>
          <Option type="QString" name="minLengthUnit" value="MM"/>
          <Option type="double" name="offsetFromAnchor" value="0"/>
          <Option type="QString" name="offsetFromAnchorMapUnitScale" value="3x:0,0,0,0,0,0"/>
          <Option type="QString" name="offsetFromAnchorUnit" value="MM"/>
          <Option type="double" name="offsetFromLabel" value="0"/>
          <Option type="QString" name="offsetFromLabelMapUnitScale" value="3x:0,0,0,0,0,0"/>
          <Option type="QString" name="offsetFromLabelUnit" value="MM"/>
        </Option>
      </callout>
    </settings>
  </labeling>
  <customproperties>
    <property key="dualview/previewExpressions" value="ROWID"/>
    <property key="embeddedWidgets/count" value="0"/>
    <property key="variableNames"/>
    <property key="variableValues"/>
  </customproperties>
  <blendMode>0</blendMode>
  <featureBlendMode>0</featureBlendMode>
  <layerOpacity>1</layerOpacity>
  <SingleCategoryDiagramRenderer attributeLegend="1" diagramType="Pie">
    <DiagramCategory penColor="#000000" scaleBasedVisibility="0" spacingUnitScale="3x:0,0,0,0,0,0" backgroundAlpha="255" diagramOrientation="Up" penWidth="0" backgroundColor="#ffffff" direction="1" barWidth="5" opacity="1" lineSizeScale="3x:0,0,0,0,0,0" labelPlacementMethod="XHeight" sizeType="MM" spacingUnit="MM" enabled="0" penAlpha="255" height="15" minimumSize="0" spacing="0" width="15" rotationOffset="270" lineSizeType="MM" showAxis="0" sizeScale="3x:0,0,0,0,0,0" minScaleDenominator="0" scaleDependency="Area" maxScaleDenominator="1e+08">
      <fontProperties description="MS Shell Dlg 2,8.25,-1,5,50,0,0,0,0,0" style=""/>
      <attribute color="#000000" label="" field=""/>
      <axisSymbol>
        <symbol type="line" name="" alpha="1" clip_to_extent="1" force_rhr="0">
          <layer enabled="1" pass="0" class="SimpleLine" locked="0">
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
            <prop v="35,35,35,255" k="line_color"/>
            <prop v="solid" k="line_style"/>
            <prop v="0.26" k="line_width"/>
            <prop v="MM" k="line_width_unit"/>
            <prop v="0" k="offset"/>
            <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
            <prop v="MM" k="offset_unit"/>
            <prop v="0" k="ring_filter"/>
            <prop v="0" k="tweak_dash_pattern_on_corners"/>
            <prop v="0" k="use_custom_dash"/>
            <prop v="3x:0,0,0,0,0,0" k="width_map_unit_scale"/>
            <data_defined_properties>
              <Option type="Map">
                <Option type="QString" name="name" value=""/>
                <Option name="properties"/>
                <Option type="QString" name="type" value="collection"/>
              </Option>
            </data_defined_properties>
          </layer>
        </symbol>
      </axisSymbol>
    </DiagramCategory>
  </SingleCategoryDiagramRenderer>
  <DiagramLayerSettings obstacle="0" zIndex="0" showAll="1" priority="0" dist="0" placement="2" linePlacementFlags="2">
    <properties>
      <Option type="Map">
        <Option type="QString" name="name" value=""/>
        <Option name="properties"/>
        <Option type="QString" name="type" value="collection"/>
      </Option>
    </properties>
  </DiagramLayerSettings>
  <geometryOptions removeDuplicateNodes="0" geometryPrecision="0">
    <activeChecks/>
    <checkConfiguration/>
  </geometryOptions>
  <legend type="default-vector"/>
  <referencedLayers/>
  <fieldConfiguration>
    <field name="ROWID" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="pipe_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="pipe_display_name" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="pipe_code" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="pipe_profile_num" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="pipe_sewerage_type" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="0: mixed" value="0"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="1: rain water" value="1"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="2: dry weather flow" value="2"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="3: transport" value="3"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="4: spillway" value="4"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="5: sinker" value="5"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="6: storage" value="6"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="7: storage tank" value="7"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="pipe_calculation_type" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="0: embedded" value="0"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="1: isolated" value="1"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="2: connected" value="2"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="3: broad crest" value="3"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="4: short crest" value="4"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="pipe_invert_level_start_point" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="pipe_invert_level_end_point" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="pipe_cross_section_definition_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="pipe_friction_value" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="pipe_friction_type" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="1: Chèzy" value="1"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="2: Manning" value="2"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="pipe_dist_calc_points" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="pipe_material" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="0: concrete" value="0"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="1: pvc" value="1"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="2: gres" value="2"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="3: cast iron" value="3"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="4: brickwork" value="4"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="5: HPE" value="5"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="6: HDPE" value="6"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="7: plate iron" value="7"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="8: steel" value="8"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="pipe_pipe_quality" configurationFlags="None">
      <editWidget type="">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="pipe_original_length" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="pipe_zoom_category" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="-1" value="-1"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="0" value="0"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="1" value="1"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="2" value="2"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="3" value="3"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="4" value="4"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="5" value="5"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="pipe_connection_node_start_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="pipe_connection_node_end_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="def_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="def_shape" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="1: rectangle" value="1"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="2: round" value="2"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="3: egg" value="3"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="5: tabulated rectangle" value="5"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="6: tabulated trapezium" value="6"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="def_width" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="def_height" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="def_code" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
  </fieldConfiguration>
  <aliases>
    <alias name="" index="0" field="ROWID"/>
    <alias name="id" index="1" field="pipe_id"/>
    <alias name="display_name" index="2" field="pipe_display_name"/>
    <alias name="code" index="3" field="pipe_code"/>
    <alias name="profile_num" index="4" field="pipe_profile_num"/>
    <alias name="sewerage_type" index="5" field="pipe_sewerage_type"/>
    <alias name="calculation_type" index="6" field="pipe_calculation_type"/>
    <alias name="invert_level_start_point" index="7" field="pipe_invert_level_start_point"/>
    <alias name="invert_level_end_point" index="8" field="pipe_invert_level_end_point"/>
    <alias name="cross_section_definition_id" index="9" field="pipe_cross_section_definition_id"/>
    <alias name="friction_value" index="10" field="pipe_friction_value"/>
    <alias name="friction_type" index="11" field="pipe_friction_type"/>
    <alias name="dist_calc_points" index="12" field="pipe_dist_calc_points"/>
    <alias name="material" index="13" field="pipe_material"/>
    <alias name="" index="14" field="pipe_pipe_quality"/>
    <alias name="original_length" index="15" field="pipe_original_length"/>
    <alias name="zoom_category" index="16" field="pipe_zoom_category"/>
    <alias name="connection_node_start_id" index="17" field="pipe_connection_node_start_id"/>
    <alias name="connection_node_end_id" index="18" field="pipe_connection_node_end_id"/>
    <alias name="id" index="19" field="def_id"/>
    <alias name="shape" index="20" field="def_shape"/>
    <alias name="width" index="21" field="def_width"/>
    <alias name="height" index="22" field="def_height"/>
    <alias name="code" index="23" field="def_code"/>
  </aliases>
  <defaults>
    <default expression="" applyOnUpdate="0" field="ROWID"/>
    <default expression="if(maximum(pipe_id) is null,1, maximum(pipe_id)+1)" applyOnUpdate="0" field="pipe_id"/>
    <default expression="'new'" applyOnUpdate="0" field="pipe_display_name"/>
    <default expression="'new'" applyOnUpdate="0" field="pipe_code"/>
    <default expression="" applyOnUpdate="0" field="pipe_profile_num"/>
    <default expression="" applyOnUpdate="0" field="pipe_sewerage_type"/>
    <default expression="1" applyOnUpdate="0" field="pipe_calculation_type"/>
    <default expression="aggregate('v2_manhole_view','mean',&quot;bottom_level&quot;, intersects($geometry,start_point(geometry(@parent))))" applyOnUpdate="0" field="pipe_invert_level_start_point"/>
    <default expression="aggregate('v2_manhole_view','mean',&quot;bottom_level&quot;, intersects($geometry,end_point(geometry(@parent))))" applyOnUpdate="0" field="pipe_invert_level_end_point"/>
    <default expression="" applyOnUpdate="0" field="pipe_cross_section_definition_id"/>
    <default expression="" applyOnUpdate="0" field="pipe_friction_value"/>
    <default expression="2" applyOnUpdate="0" field="pipe_friction_type"/>
    <default expression="10000" applyOnUpdate="0" field="pipe_dist_calc_points"/>
    <default expression="" applyOnUpdate="0" field="pipe_material"/>
    <default expression="" applyOnUpdate="0" field="pipe_pipe_quality"/>
    <default expression="" applyOnUpdate="0" field="pipe_original_length"/>
    <default expression="2" applyOnUpdate="0" field="pipe_zoom_category"/>
    <default expression="if(aggregate('v2_connection_nodes','min',&quot;id&quot;, intersects($geometry,start_point(geometry(@parent)))) is null, 'Created automatically',aggregate('v2_connection_nodes','min',&quot;id&quot;, intersects($geometry,start_point(geometry(@parent)))))" applyOnUpdate="0" field="pipe_connection_node_start_id"/>
    <default expression="if(aggregate('v2_connection_nodes','min',&quot;id&quot;, intersects($geometry,end_point(geometry(@parent)))) is null, 'Created automatically',aggregate('v2_connection_nodes','min',&quot;id&quot;, intersects($geometry,end_point(geometry(@parent)))))" applyOnUpdate="0" field="pipe_connection_node_end_id"/>
    <default expression="" applyOnUpdate="0" field="def_id"/>
    <default expression="" applyOnUpdate="0" field="def_shape"/>
    <default expression="" applyOnUpdate="0" field="def_width"/>
    <default expression="" applyOnUpdate="0" field="def_height"/>
    <default expression="" applyOnUpdate="0" field="def_code"/>
  </defaults>
  <constraints>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="ROWID"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="pipe_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="pipe_display_name"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="pipe_code"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="pipe_profile_num"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="pipe_sewerage_type"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="pipe_calculation_type"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="pipe_invert_level_start_point"/>
    <constraint unique_strength="0" exp_strength="2" notnull_strength="2" constraints="5" field="pipe_invert_level_end_point"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="pipe_cross_section_definition_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="pipe_friction_value"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="pipe_friction_type"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="pipe_dist_calc_points"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="pipe_material"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="pipe_pipe_quality"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="pipe_original_length"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="pipe_zoom_category"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="pipe_connection_node_start_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="pipe_connection_node_end_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="def_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="def_shape"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="def_width"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="def_height"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="def_code"/>
  </constraints>
  <constraintExpressions>
    <constraint desc="" exp="" field="ROWID"/>
    <constraint desc="" exp="" field="pipe_id"/>
    <constraint desc="" exp="" field="pipe_display_name"/>
    <constraint desc="" exp="" field="pipe_code"/>
    <constraint desc="" exp="" field="pipe_profile_num"/>
    <constraint desc="" exp="" field="pipe_sewerage_type"/>
    <constraint desc="" exp="" field="pipe_calculation_type"/>
    <constraint desc="" exp="" field="pipe_invert_level_start_point"/>
    <constraint desc="" exp="&quot;invert_level_end_point&quot; is not null" field="pipe_invert_level_end_point"/>
    <constraint desc="" exp="" field="pipe_cross_section_definition_id"/>
    <constraint desc="" exp="" field="pipe_friction_value"/>
    <constraint desc="" exp="" field="pipe_friction_type"/>
    <constraint desc="" exp="" field="pipe_dist_calc_points"/>
    <constraint desc="" exp="" field="pipe_material"/>
    <constraint desc="" exp="" field="pipe_pipe_quality"/>
    <constraint desc="" exp="" field="pipe_original_length"/>
    <constraint desc="" exp="" field="pipe_zoom_category"/>
    <constraint desc="" exp="" field="pipe_connection_node_start_id"/>
    <constraint desc="" exp="" field="pipe_connection_node_end_id"/>
    <constraint desc="" exp="" field="def_id"/>
    <constraint desc="" exp="" field="def_shape"/>
    <constraint desc="" exp="" field="def_width"/>
    <constraint desc="" exp="" field="def_height"/>
    <constraint desc="" exp="" field="def_code"/>
  </constraintExpressions>
  <expressionfields/>
  <attributeactions>
    <defaultAction key="Canvas" value="{00000000-0000-0000-0000-000000000000}"/>
  </attributeactions>
  <attributetableconfig actionWidgetStyle="dropDown" sortExpression="" sortOrder="0">
    <columns>
      <column width="-1" type="field" name="ROWID" hidden="0"/>
      <column width="-1" type="field" name="pipe_id" hidden="0"/>
      <column width="-1" type="field" name="pipe_display_name" hidden="0"/>
      <column width="-1" type="field" name="pipe_code" hidden="0"/>
      <column width="-1" type="field" name="pipe_profile_num" hidden="0"/>
      <column width="-1" type="field" name="pipe_sewerage_type" hidden="0"/>
      <column width="-1" type="field" name="pipe_calculation_type" hidden="0"/>
      <column width="-1" type="field" name="pipe_invert_level_start_point" hidden="0"/>
      <column width="-1" type="field" name="pipe_invert_level_end_point" hidden="0"/>
      <column width="-1" type="field" name="pipe_cross_section_definition_id" hidden="0"/>
      <column width="-1" type="field" name="pipe_friction_value" hidden="0"/>
      <column width="-1" type="field" name="pipe_friction_type" hidden="0"/>
      <column width="-1" type="field" name="pipe_dist_calc_points" hidden="0"/>
      <column width="-1" type="field" name="pipe_material" hidden="0"/>
      <column width="-1" type="field" name="pipe_original_length" hidden="0"/>
      <column width="-1" type="field" name="pipe_zoom_category" hidden="0"/>
      <column width="-1" type="field" name="pipe_connection_node_start_id" hidden="0"/>
      <column width="-1" type="field" name="pipe_connection_node_end_id" hidden="0"/>
      <column width="-1" type="field" name="def_id" hidden="0"/>
      <column width="-1" type="field" name="def_shape" hidden="0"/>
      <column width="-1" type="field" name="def_width" hidden="0"/>
      <column width="-1" type="field" name="def_height" hidden="0"/>
      <column width="-1" type="field" name="def_code" hidden="0"/>
      <column width="-1" type="actions" hidden="1"/>
    </columns>
  </attributetableconfig>
  <conditionalstyles>
    <rowstyles/>
    <fieldstyles/>
  </conditionalstyles>
  <storedexpressions/>
  <editform tolerant="1">C:/Users/chris.kerklaan/Downloads/test_protocol_v21</editform>
  <editforminit/>
  <editforminitcodesource>0</editforminitcodesource>
  <editforminitfilepath>C:/Users/chris.kerklaan/Downloads/test_protocol_v21</editforminitfilepath>
  <editforminitcode><![CDATA[# -*- coding: utf-8 -*-
"""
Formulieren van QGIS mogen een functie van Python hebben die wordt aangeroepen wanneer het formulier wordt geopend.

Gebruik deze functie om extra logica aan uw formulieren toe te voegen.

Voer de naam van de functie in in het veld "Python Init functie".
Een voorbeeld volgt:
"""
from PyQt4.QtGui import QWidget

def my_form_open(dialog, layer, feature):
	geom = feature.geometry()
	control = dialog.findChild(QWidget, "MyLineEdit")
]]></editforminitcode>
  <featformsuppress>0</featformsuppress>
  <editorlayout>tablayout</editorlayout>
  <attributeEditorForm>
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Pipe view" visibilityExpression="" groupBox="0" showLabel="1">
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="General" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="pipe_id" showLabel="1" index="1"/>
        <attributeEditorField name="pipe_display_name" showLabel="1" index="2"/>
        <attributeEditorField name="pipe_code" showLabel="1" index="3"/>
        <attributeEditorField name="pipe_calculation_type" showLabel="1" index="6"/>
        <attributeEditorField name="pipe_dist_calc_points" showLabel="1" index="12"/>
      </attributeEditorContainer>
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Characteristics" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="pipe_invert_level_start_point" showLabel="1" index="7"/>
        <attributeEditorField name="pipe_invert_level_end_point" showLabel="1" index="8"/>
        <attributeEditorField name="pipe_friction_value" showLabel="1" index="10"/>
        <attributeEditorField name="pipe_friction_type" showLabel="1" index="11"/>
        <attributeEditorField name="pipe_material" showLabel="1" index="13"/>
      </attributeEditorContainer>
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Cross section definition" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="pipe_cross_section_definition_id" showLabel="1" index="9"/>
        <attributeEditorField name="def_shape" showLabel="1" index="20"/>
        <attributeEditorField name="def_width" showLabel="1" index="21"/>
        <attributeEditorField name="def_height" showLabel="1" index="22"/>
        <attributeEditorField name="def_code" showLabel="1" index="23"/>
      </attributeEditorContainer>
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Visualization" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="pipe_sewerage_type" showLabel="1" index="5"/>
        <attributeEditorField name="pipe_zoom_category" showLabel="1" index="16"/>
      </attributeEditorContainer>
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Connection nodes" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="pipe_connection_node_start_id" showLabel="1" index="17"/>
        <attributeEditorField name="pipe_connection_node_end_id" showLabel="1" index="18"/>
      </attributeEditorContainer>
    </attributeEditorContainer>
  </attributeEditorForm>
  <editable>
    <field name="ROWID" editable="1"/>
    <field name="def_code" editable="0"/>
    <field name="def_height" editable="0"/>
    <field name="def_id" editable="1"/>
    <field name="def_shape" editable="0"/>
    <field name="def_width" editable="0"/>
    <field name="pipe_calculation_type" editable="1"/>
    <field name="pipe_code" editable="1"/>
    <field name="pipe_connection_node_end_id" editable="0"/>
    <field name="pipe_connection_node_start_id" editable="0"/>
    <field name="pipe_cross_section_definition_id" editable="1"/>
    <field name="pipe_display_name" editable="1"/>
    <field name="pipe_dist_calc_points" editable="1"/>
    <field name="pipe_friction_type" editable="1"/>
    <field name="pipe_friction_value" editable="1"/>
    <field name="pipe_id" editable="1"/>
    <field name="pipe_invert_level_end_point" editable="1"/>
    <field name="pipe_invert_level_start_point" editable="1"/>
    <field name="pipe_material" editable="1"/>
    <field name="pipe_original_length" editable="1"/>
    <field name="pipe_pipe_quality" editable="1"/>
    <field name="pipe_profile_num" editable="1"/>
    <field name="pipe_sewerage_type" editable="1"/>
    <field name="pipe_zoom_category" editable="1"/>
  </editable>
  <labelOnTop>
    <field name="ROWID" labelOnTop="0"/>
    <field name="def_code" labelOnTop="0"/>
    <field name="def_height" labelOnTop="0"/>
    <field name="def_id" labelOnTop="0"/>
    <field name="def_shape" labelOnTop="0"/>
    <field name="def_width" labelOnTop="0"/>
    <field name="pipe_calculation_type" labelOnTop="0"/>
    <field name="pipe_code" labelOnTop="0"/>
    <field name="pipe_connection_node_end_id" labelOnTop="0"/>
    <field name="pipe_connection_node_start_id" labelOnTop="0"/>
    <field name="pipe_cross_section_definition_id" labelOnTop="0"/>
    <field name="pipe_display_name" labelOnTop="0"/>
    <field name="pipe_dist_calc_points" labelOnTop="0"/>
    <field name="pipe_friction_type" labelOnTop="0"/>
    <field name="pipe_friction_value" labelOnTop="0"/>
    <field name="pipe_id" labelOnTop="0"/>
    <field name="pipe_invert_level_end_point" labelOnTop="0"/>
    <field name="pipe_invert_level_start_point" labelOnTop="0"/>
    <field name="pipe_material" labelOnTop="0"/>
    <field name="pipe_original_length" labelOnTop="0"/>
    <field name="pipe_pipe_quality" labelOnTop="0"/>
    <field name="pipe_profile_num" labelOnTop="0"/>
    <field name="pipe_sewerage_type" labelOnTop="0"/>
    <field name="pipe_zoom_category" labelOnTop="0"/>
  </labelOnTop>
  <dataDefinedFieldProperties/>
  <widgets/>
  <previewExpression>"ROWID"</previewExpression>
  <mapTip>display_name</mapTip>
  <layerGeometryType>1</layerGeometryType>
</qgis>

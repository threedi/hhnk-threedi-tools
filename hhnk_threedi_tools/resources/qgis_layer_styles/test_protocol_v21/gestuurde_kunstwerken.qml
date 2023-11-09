<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis simplifyLocal="1" readOnly="0" labelsEnabled="1" styleCategories="AllStyleCategories" simplifyAlgorithm="0" hasScaleBasedVisibilityFlag="0" minScale="0" simplifyMaxScale="1" version="3.16.11-Hannover" maxScale="0" simplifyDrawingTol="1" simplifyDrawingHints="1">
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
  <renderer-v2 symbollevels="0" forceraster="0" type="singleSymbol" enableorderby="0">
    <symbols>
      <symbol type="line" name="0" alpha="1" clip_to_extent="1" force_rhr="0">
        <layer enabled="1" pass="0" class="ArrowLine" locked="0">
          <prop v="1" k="arrow_start_width"/>
          <prop v="MM" k="arrow_start_width_unit"/>
          <prop v="3x:0,0,0,0,0,0" k="arrow_start_width_unit_scale"/>
          <prop v="0" k="arrow_type"/>
          <prop v="1" k="arrow_width"/>
          <prop v="MM" k="arrow_width_unit"/>
          <prop v="3x:0,0,0,0,0,0" k="arrow_width_unit_scale"/>
          <prop v="1.5" k="head_length"/>
          <prop v="MM" k="head_length_unit"/>
          <prop v="3x:0,0,0,0,0,0" k="head_length_unit_scale"/>
          <prop v="1.5" k="head_thickness"/>
          <prop v="MM" k="head_thickness_unit"/>
          <prop v="3x:0,0,0,0,0,0" k="head_thickness_unit_scale"/>
          <prop v="0" k="head_type"/>
          <prop v="1" k="is_curved"/>
          <prop v="1" k="is_repeated"/>
          <prop v="0" k="offset"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_unit_scale"/>
          <prop v="0" k="ring_filter"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option name="properties"/>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
          <symbol type="fill" name="@0@0" alpha="1" clip_to_extent="1" force_rhr="0">
            <layer enabled="1" pass="0" class="SimpleFill" locked="0">
              <prop v="3x:0,0,0,0,0,0" k="border_width_map_unit_scale"/>
              <prop v="255,174,0,255" k="color"/>
              <prop v="bevel" k="joinstyle"/>
              <prop v="0,0" k="offset"/>
              <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
              <prop v="MM" k="offset_unit"/>
              <prop v="213,180,60,255" k="outline_color"/>
              <prop v="solid" k="outline_style"/>
              <prop v="0.26" k="outline_width"/>
              <prop v="MM" k="outline_width_unit"/>
              <prop v="solid" k="style"/>
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
    </symbols>
    <rotation/>
    <sizescale/>
  </renderer-v2>
  <labeling type="simple">
    <settings calloutType="simple">
      <text-style fontLetterSpacing="0" fontWeight="50" useSubstitutions="0" blendMode="0" multilineHeight="1" previewBkgrdColor="255,255,255,255" isExpression="1" fontWordSpacing="0" fontStrikeout="0" textColor="0,0,0,255" fontSize="8.25" textOrientation="horizontal" fontKerning="1" fontFamily="MS Shell Dlg 2" allowHtml="0" fontItalic="0" fontSizeMapUnitScale="3x:0,0,0,0,0,0" capitalization="0" fontUnderline="0" fieldName=" &quot;code&quot;  || '\n' || 'start: ' ||  &quot;start_action_value&quot; || '\n' ||&#xd;&#xa;  'min: ' || &quot;min_action_value&quot; ||&#xd;&#xa;  '\n' || 'max: ' || &quot;max_action_value&quot; || '\n'" fontSizeUnit="Point" textOpacity="1" namedStyle="Standaard">
        <text-buffer bufferOpacity="1" bufferColor="255,255,255,255" bufferSizeUnits="MM" bufferNoFill="1" bufferDraw="1" bufferSizeMapUnitScale="3x:0,0,0,0,0,0" bufferSize="1" bufferBlendMode="0" bufferJoinStyle="128"/>
        <text-mask maskSize="1.5" maskOpacity="1" maskType="0" maskEnabled="0" maskSizeMapUnitScale="3x:0,0,0,0,0,0" maskedSymbolLayers="" maskSizeUnits="MM" maskJoinStyle="128"/>
        <background shapeRadiiUnit="MM" shapeRadiiMapUnitScale="3x:0,0,0,0,0,0" shapeOffsetX="0" shapeOffsetY="0" shapeBorderColor="128,128,128,255" shapeSizeUnit="MM" shapeType="0" shapeOffsetMapUnitScale="3x:0,0,0,0,0,0" shapeRotation="0" shapeBorderWidthMapUnitScale="3x:0,0,0,0,0,0" shapeSizeType="0" shapeSizeMapUnitScale="3x:0,0,0,0,0,0" shapeRadiiX="0" shapeJoinStyle="64" shapeBlendMode="0" shapeBorderWidth="0" shapeDraw="0" shapeRotationType="0" shapeRadiiY="0" shapeBorderWidthUnit="MM" shapeSizeY="0" shapeOpacity="1" shapeOffsetUnit="MM" shapeSVGFile="" shapeSizeX="0" shapeFillColor="255,255,255,255">
          <symbol type="marker" name="markerSymbol" alpha="1" clip_to_extent="1" force_rhr="0">
            <layer enabled="1" pass="0" class="SimpleMarker" locked="0">
              <prop v="0" k="angle"/>
              <prop v="196,60,57,255" k="color"/>
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
      <placement placementFlags="10" overrunDistanceMapUnitScale="3x:0,0,0,0,0,0" offsetType="0" lineAnchorType="0" centroidInside="0" yOffset="0" repeatDistanceUnits="MM" fitInPolygonOnly="0" repeatDistanceMapUnitScale="3x:0,0,0,0,0,0" dist="0" polygonPlacementFlags="2" geometryGeneratorEnabled="0" layerType="LineGeometry" maxCurvedCharAngleIn="25" distUnits="MM" priority="5" lineAnchorPercent="0.5" overrunDistanceUnit="MM" distMapUnitScale="3x:0,0,0,0,0,0" centroidWhole="0" maxCurvedCharAngleOut="-25" rotationAngle="0" geometryGenerator="" offsetUnits="MM" labelOffsetMapUnitScale="3x:0,0,0,0,0,0" predefinedPositionOrder="TR,TL,BR,BL,R,L,TSR,BSR" repeatDistance="0" xOffset="0" preserveRotation="1" overrunDistance="0" quadOffset="4" placement="2" geometryGeneratorType="PointGeometry"/>
      <rendering displayAll="1" minFeatureSize="0" labelPerPart="0" drawLabels="1" fontMaxPixelSize="10000" obstacleType="1" scaleVisibility="1" scaleMin="0" fontMinPixelSize="3" scaleMax="41486" zIndex="0" upsidedownLabels="0" fontLimitPixelSize="0" maxNumLabels="2000" mergeLines="1" obstacle="1" limitNumLabels="0" obstacleFactor="1"/>
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
  <customproperties/>
  <blendMode>0</blendMode>
  <featureBlendMode>0</featureBlendMode>
  <layerOpacity>1</layerOpacity>
  <geometryOptions removeDuplicateNodes="0" geometryPrecision="0">
    <activeChecks/>
    <checkConfiguration/>
  </geometryOptions>
  <legend type="default-vector"/>
  <referencedLayers/>
  <fieldConfiguration>
    <field name="fid" configurationFlags="None">
      <editWidget type="">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="control_id" configurationFlags="None">
      <editWidget type="">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="action_table" configurationFlags="None">
      <editWidget type="">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="target_type" configurationFlags="None">
      <editWidget type="">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="target_id" configurationFlags="None">
      <editWidget type="">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="code" configurationFlags="None">
      <editWidget type="">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="start_action_value" configurationFlags="None">
      <editWidget type="">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="min_action_value" configurationFlags="None">
      <editWidget type="">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="max_action_value" configurationFlags="None">
      <editWidget type="">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="hdb_streefpeil" configurationFlags="None">
      <editWidget type="">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="hdb_kruin_min" configurationFlags="None">
      <editWidget type="">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="hdb_kruin_max" configurationFlags="None">
      <editWidget type="">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
  </fieldConfiguration>
  <aliases>
    <alias name="" index="0" field="fid"/>
    <alias name="" index="1" field="control_id"/>
    <alias name="" index="2" field="action_table"/>
    <alias name="" index="3" field="target_type"/>
    <alias name="" index="4" field="target_id"/>
    <alias name="" index="5" field="code"/>
    <alias name="" index="6" field="start_action_value"/>
    <alias name="" index="7" field="min_action_value"/>
    <alias name="" index="8" field="max_action_value"/>
    <alias name="" index="9" field="hdb_streefpeil"/>
    <alias name="" index="10" field="hdb_kruin_min"/>
    <alias name="" index="11" field="hdb_kruin_max"/>
  </aliases>
  <defaults>
    <default expression="" applyOnUpdate="0" field="fid"/>
    <default expression="" applyOnUpdate="0" field="control_id"/>
    <default expression="" applyOnUpdate="0" field="action_table"/>
    <default expression="" applyOnUpdate="0" field="target_type"/>
    <default expression="" applyOnUpdate="0" field="target_id"/>
    <default expression="" applyOnUpdate="0" field="code"/>
    <default expression="" applyOnUpdate="0" field="start_action_value"/>
    <default expression="" applyOnUpdate="0" field="min_action_value"/>
    <default expression="" applyOnUpdate="0" field="max_action_value"/>
    <default expression="" applyOnUpdate="0" field="hdb_streefpeil"/>
    <default expression="" applyOnUpdate="0" field="hdb_kruin_min"/>
    <default expression="" applyOnUpdate="0" field="hdb_kruin_max"/>
  </defaults>
  <constraints>
    <constraint unique_strength="1" exp_strength="0" notnull_strength="1" constraints="3" field="fid"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="control_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="action_table"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="target_type"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="target_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="code"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="start_action_value"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="min_action_value"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="max_action_value"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="hdb_streefpeil"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="hdb_kruin_min"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="hdb_kruin_max"/>
  </constraints>
  <constraintExpressions>
    <constraint desc="" exp="" field="fid"/>
    <constraint desc="" exp="" field="control_id"/>
    <constraint desc="" exp="" field="action_table"/>
    <constraint desc="" exp="" field="target_type"/>
    <constraint desc="" exp="" field="target_id"/>
    <constraint desc="" exp="" field="code"/>
    <constraint desc="" exp="" field="start_action_value"/>
    <constraint desc="" exp="" field="min_action_value"/>
    <constraint desc="" exp="" field="max_action_value"/>
    <constraint desc="" exp="" field="hdb_streefpeil"/>
    <constraint desc="" exp="" field="hdb_kruin_min"/>
    <constraint desc="" exp="" field="hdb_kruin_max"/>
  </constraintExpressions>
  <expressionfields/>
  <attributeactions>
    <defaultAction key="Canvas" value="{00000000-0000-0000-0000-000000000000}"/>
  </attributeactions>
  <attributetableconfig actionWidgetStyle="dropDown" sortExpression="" sortOrder="0">
    <columns/>
  </attributetableconfig>
  <conditionalstyles>
    <rowstyles/>
    <fieldstyles/>
  </conditionalstyles>
  <storedexpressions/>
  <editform tolerant="1"></editform>
  <editforminit/>
  <editforminitcodesource>0</editforminitcodesource>
  <editforminitfilepath></editforminitfilepath>
  <editforminitcode><![CDATA[]]></editforminitcode>
  <featformsuppress>0</featformsuppress>
  <editorlayout>generatedlayout</editorlayout>
  <editable/>
  <labelOnTop/>
  <dataDefinedFieldProperties/>
  <widgets/>
  <previewExpression></previewExpression>
  <mapTip></mapTip>
  <layerGeometryType>1</layerGeometryType>
</qgis>

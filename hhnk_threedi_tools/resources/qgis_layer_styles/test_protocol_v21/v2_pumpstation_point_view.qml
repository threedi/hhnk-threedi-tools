<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis simplifyLocal="1" readOnly="0" labelsEnabled="0" styleCategories="AllStyleCategories" simplifyAlgorithm="0" hasScaleBasedVisibilityFlag="0" minScale="100000000" simplifyMaxScale="1" version="3.16.11-Hannover" maxScale="-4.65661e-10" simplifyDrawingTol="1" simplifyDrawingHints="0">
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
      <symbol type="marker" name="0" alpha="1" clip_to_extent="1" force_rhr="0">
        <layer enabled="1" pass="0" class="SimpleMarker" locked="0">
          <prop v="0" k="angle"/>
          <prop v="185,185,185,255" k="color"/>
          <prop v="1" k="horizontal_anchor_point"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="pentagon" k="name"/>
          <prop v="0,0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="77,77,77,255" k="outline_color"/>
          <prop v="solid" k="outline_style"/>
          <prop v="0.6" k="outline_width"/>
          <prop v="3x:0,0,0,0,0,0" k="outline_width_map_unit_scale"/>
          <prop v="MM" k="outline_width_unit"/>
          <prop v="diameter" k="scale_method"/>
          <prop v="4" k="size"/>
          <prop v="3x:0,0,0,0,0,0" k="size_map_unit_scale"/>
          <prop v="MM" k="size_unit"/>
          <prop v="1" k="vertical_anchor_point"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option type="Map" name="properties">
                <Option type="Map" name="size">
                  <Option type="bool" name="active" value="true"/>
                  <Option type="QString" name="expression" value="if(@map_scale&lt;10000, 4,3)"/>
                  <Option type="int" name="type" value="3"/>
                </Option>
              </Option>
              <Option type="QString" name="type" value="collection"/>
            </Option>
          </data_defined_properties>
        </layer>
      </symbol>
    </symbols>
    <rotation/>
    <sizescale/>
  </renderer-v2>
  <customproperties>
    <property key="dualview/previewExpressions" value="display_name"/>
    <property key="embeddedWidgets/count" value="0"/>
    <property key="variableNames"/>
    <property key="variableValues"/>
  </customproperties>
  <blendMode>0</blendMode>
  <featureBlendMode>0</featureBlendMode>
  <layerOpacity>1</layerOpacity>
  <SingleCategoryDiagramRenderer attributeLegend="1" diagramType="Pie">
    <DiagramCategory penColor="#000000" scaleBasedVisibility="0" spacingUnitScale="3x:0,0,0,0,0,0" backgroundAlpha="255" diagramOrientation="Up" penWidth="0" backgroundColor="#ffffff" direction="1" barWidth="5" opacity="1" lineSizeScale="3x:0,0,0,0,0,0" labelPlacementMethod="XHeight" sizeType="MM" spacingUnit="MM" enabled="0" penAlpha="255" height="15" minimumSize="0" spacing="0" width="15" rotationOffset="270" lineSizeType="MM" showAxis="0" sizeScale="3x:0,0,0,0,0,0" minScaleDenominator="-4.65661e-10" scaleDependency="Area" maxScaleDenominator="1e+08">
      <fontProperties description="MS Shell Dlg 2,7.5,-1,5,50,0,0,0,0,0" style=""/>
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
  <DiagramLayerSettings obstacle="0" zIndex="0" showAll="1" priority="0" dist="0" placement="0" linePlacementFlags="2">
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
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="pump_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="display_name" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="code" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="classification" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="sewerage" configurationFlags="None">
      <editWidget type="CheckBox">
        <config>
          <Option type="Map">
            <Option type="QString" name="CheckedState" value="1"/>
            <Option type="QString" name="UncheckedState" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="start_level" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="lower_stop_level" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="upper_stop_level" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="capacity" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="zoom_category" configurationFlags="None">
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
    <field name="connection_node_start_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="connection_node_end_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="type" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="1: pump reacts only on suction side" value="1"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="2: pump reacts only on delivery side" value="2"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="connection_node_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="storage_area" configurationFlags="None">
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
    <alias name="id" index="1" field="pump_id"/>
    <alias name="" index="2" field="display_name"/>
    <alias name="" index="3" field="code"/>
    <alias name="" index="4" field="classification"/>
    <alias name="" index="5" field="sewerage"/>
    <alias name="" index="6" field="start_level"/>
    <alias name="" index="7" field="lower_stop_level"/>
    <alias name="" index="8" field="upper_stop_level"/>
    <alias name="" index="9" field="capacity"/>
    <alias name="" index="10" field="zoom_category"/>
    <alias name="" index="11" field="connection_node_start_id"/>
    <alias name="" index="12" field="connection_node_end_id"/>
    <alias name="" index="13" field="type"/>
    <alias name="id" index="14" field="connection_node_id"/>
    <alias name="" index="15" field="storage_area"/>
  </aliases>
  <defaults>
    <default expression="" applyOnUpdate="0" field="ROWID"/>
    <default expression="if(maximum(pump_id) is null,1, maximum(pump_id)+1)" applyOnUpdate="0" field="pump_id"/>
    <default expression="'new'" applyOnUpdate="0" field="display_name"/>
    <default expression="'new'" applyOnUpdate="0" field="code"/>
    <default expression="" applyOnUpdate="0" field="classification"/>
    <default expression="" applyOnUpdate="0" field="sewerage"/>
    <default expression="" applyOnUpdate="0" field="start_level"/>
    <default expression="" applyOnUpdate="0" field="lower_stop_level"/>
    <default expression="" applyOnUpdate="0" field="upper_stop_level"/>
    <default expression="" applyOnUpdate="0" field="capacity"/>
    <default expression="2" applyOnUpdate="0" field="zoom_category"/>
    <default expression="'filled automatically'" applyOnUpdate="0" field="connection_node_start_id"/>
    <default expression="'if you want to use an endpoint use v2_pumpstation_view'" applyOnUpdate="0" field="connection_node_end_id"/>
    <default expression="1" applyOnUpdate="0" field="type"/>
    <default expression="'filled automatically'" applyOnUpdate="0" field="connection_node_id"/>
    <default expression="" applyOnUpdate="0" field="storage_area"/>
  </defaults>
  <constraints>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="ROWID"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="pump_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="display_name"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="code"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="classification"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="sewerage"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="start_level"/>
    <constraint unique_strength="0" exp_strength="2" notnull_strength="2" constraints="5" field="lower_stop_level"/>
    <constraint unique_strength="0" exp_strength="2" notnull_strength="0" constraints="4" field="upper_stop_level"/>
    <constraint unique_strength="0" exp_strength="2" notnull_strength="2" constraints="5" field="capacity"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="zoom_category"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="connection_node_start_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="connection_node_end_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="type"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="connection_node_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="storage_area"/>
  </constraints>
  <constraintExpressions>
    <constraint desc="" exp="" field="ROWID"/>
    <constraint desc="" exp="" field="pump_id"/>
    <constraint desc="" exp="" field="display_name"/>
    <constraint desc="" exp="" field="code"/>
    <constraint desc="" exp="" field="classification"/>
    <constraint desc="" exp="" field="sewerage"/>
    <constraint desc="" exp="" field="start_level"/>
    <constraint desc="" exp="&quot;lower_stop_level&quot;&lt;&quot;start_level&quot;" field="lower_stop_level"/>
    <constraint desc="" exp="&quot;upper_stop_level&quot;>&quot;start_level&quot; or &quot;upper_stop_level&quot; is null&#xd;&#xa;" field="upper_stop_level"/>
    <constraint desc="" exp="&quot;capacity&quot;>=0" field="capacity"/>
    <constraint desc="" exp="" field="zoom_category"/>
    <constraint desc="" exp="" field="connection_node_start_id"/>
    <constraint desc="" exp="" field="connection_node_end_id"/>
    <constraint desc="" exp="" field="type"/>
    <constraint desc="" exp="" field="connection_node_id"/>
    <constraint desc="" exp="" field="storage_area"/>
  </constraintExpressions>
  <expressionfields/>
  <attributeactions>
    <defaultAction key="Canvas" value="{00000000-0000-0000-0000-000000000000}"/>
  </attributeactions>
  <attributetableconfig actionWidgetStyle="dropDown" sortExpression="" sortOrder="0">
    <columns>
      <column width="-1" type="field" name="ROWID" hidden="0"/>
      <column width="-1" type="field" name="pump_id" hidden="0"/>
      <column width="-1" type="field" name="display_name" hidden="0"/>
      <column width="-1" type="field" name="code" hidden="0"/>
      <column width="-1" type="field" name="classification" hidden="0"/>
      <column width="-1" type="field" name="sewerage" hidden="0"/>
      <column width="-1" type="field" name="start_level" hidden="0"/>
      <column width="-1" type="field" name="lower_stop_level" hidden="0"/>
      <column width="-1" type="field" name="upper_stop_level" hidden="0"/>
      <column width="-1" type="field" name="capacity" hidden="0"/>
      <column width="-1" type="field" name="zoom_category" hidden="0"/>
      <column width="-1" type="field" name="connection_node_start_id" hidden="0"/>
      <column width="-1" type="field" name="connection_node_end_id" hidden="0"/>
      <column width="-1" type="field" name="type" hidden="0"/>
      <column width="-1" type="field" name="connection_node_id" hidden="0"/>
      <column width="-1" type="field" name="storage_area" hidden="0"/>
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
QGIS forms can have a Python function that is called when the form is
opened.

Use this function to add extra logic to your forms.

Enter the name of the function in the "Python Init function"
field.
An example follows:
"""
from PyQt4.QtGui import QWidget

def my_form_open(dialog, layer, feature):
	geom = feature.geometry()
	control = dialog.findChild(QWidget, "MyLineEdit")
]]></editforminitcode>
  <featformsuppress>0</featformsuppress>
  <editorlayout>tablayout</editorlayout>
  <attributeEditorForm>
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Pumpstation point view" visibilityExpression="" groupBox="0" showLabel="1">
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="General" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="pump_id" showLabel="1" index="1"/>
        <attributeEditorField name="display_name" showLabel="1" index="2"/>
        <attributeEditorField name="code" showLabel="1" index="3"/>
      </attributeEditorContainer>
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Characteristics" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="start_level" showLabel="1" index="6"/>
        <attributeEditorField name="lower_stop_level" showLabel="1" index="7"/>
        <attributeEditorField name="upper_stop_level" showLabel="1" index="8"/>
        <attributeEditorField name="capacity" showLabel="1" index="9"/>
        <attributeEditorField name="type" showLabel="1" index="13"/>
        <attributeEditorField name="storage_area" showLabel="1" index="15"/>
      </attributeEditorContainer>
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Visualization" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="sewerage" showLabel="1" index="5"/>
        <attributeEditorField name="zoom_category" showLabel="1" index="10"/>
      </attributeEditorContainer>
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Connection nodes" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="connection_node_id" showLabel="1" index="14"/>
        <attributeEditorField name="connection_node_start_id" showLabel="1" index="11"/>
        <attributeEditorField name="connection_node_end_id" showLabel="1" index="12"/>
      </attributeEditorContainer>
    </attributeEditorContainer>
  </attributeEditorForm>
  <editable>
    <field name="ROWID" editable="1"/>
    <field name="capacity" editable="1"/>
    <field name="classification" editable="1"/>
    <field name="code" editable="1"/>
    <field name="connection_node_end_id" editable="0"/>
    <field name="connection_node_id" editable="0"/>
    <field name="connection_node_start_id" editable="0"/>
    <field name="display_name" editable="1"/>
    <field name="lower_stop_level" editable="1"/>
    <field name="pump_id" editable="1"/>
    <field name="sewerage" editable="1"/>
    <field name="start_level" editable="1"/>
    <field name="storage_area" editable="1"/>
    <field name="type" editable="1"/>
    <field name="upper_stop_level" editable="1"/>
    <field name="zoom_category" editable="1"/>
  </editable>
  <labelOnTop>
    <field name="ROWID" labelOnTop="0"/>
    <field name="capacity" labelOnTop="0"/>
    <field name="classification" labelOnTop="0"/>
    <field name="code" labelOnTop="0"/>
    <field name="connection_node_end_id" labelOnTop="0"/>
    <field name="connection_node_id" labelOnTop="0"/>
    <field name="connection_node_start_id" labelOnTop="0"/>
    <field name="display_name" labelOnTop="0"/>
    <field name="lower_stop_level" labelOnTop="0"/>
    <field name="pump_id" labelOnTop="0"/>
    <field name="sewerage" labelOnTop="0"/>
    <field name="start_level" labelOnTop="0"/>
    <field name="storage_area" labelOnTop="0"/>
    <field name="type" labelOnTop="0"/>
    <field name="upper_stop_level" labelOnTop="0"/>
    <field name="zoom_category" labelOnTop="0"/>
  </labelOnTop>
  <dataDefinedFieldProperties/>
  <widgets/>
  <previewExpression>"display_name"</previewExpression>
  <mapTip></mapTip>
  <layerGeometryType>0</layerGeometryType>
</qgis>

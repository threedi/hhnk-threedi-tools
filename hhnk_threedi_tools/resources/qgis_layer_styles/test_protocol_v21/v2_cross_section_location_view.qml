<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis simplifyLocal="1" readOnly="0" labelsEnabled="0" styleCategories="AllStyleCategories" simplifyAlgorithm="0" hasScaleBasedVisibilityFlag="0" minScale="100000000" simplifyMaxScale="1" version="3.16.11-Hannover" maxScale="0" simplifyDrawingTol="1" simplifyDrawingHints="0">
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
          <prop v="19,61,142,255" k="color"/>
          <prop v="1" k="horizontal_anchor_point"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="diamond" k="name"/>
          <prop v="0,0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="0,0,0,255" k="outline_color"/>
          <prop v="solid" k="outline_style"/>
          <prop v="0" k="outline_width"/>
          <prop v="3x:0,0,0,0,0,0" k="outline_width_map_unit_scale"/>
          <prop v="MM" k="outline_width_unit"/>
          <prop v="area" k="scale_method"/>
          <prop v="2" k="size"/>
          <prop v="3x:0,0,0,0,0,0" k="size_map_unit_scale"/>
          <prop v="MM" k="size_unit"/>
          <prop v="1" k="vertical_anchor_point"/>
          <data_defined_properties>
            <Option type="Map">
              <Option type="QString" name="name" value=""/>
              <Option type="Map" name="properties">
                <Option type="Map" name="size">
                  <Option type="bool" name="active" value="true"/>
                  <Option type="QString" name="expression" value="if(@map_scale&lt;10000, 2,0.5)"/>
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
    <property key="dualview/previewExpressions" value="id"/>
    <property key="embeddedWidgets/count" value="0"/>
    <property key="variableNames"/>
    <property key="variableValues"/>
  </customproperties>
  <blendMode>0</blendMode>
  <featureBlendMode>0</featureBlendMode>
  <layerOpacity>1</layerOpacity>
  <SingleCategoryDiagramRenderer attributeLegend="1" diagramType="Histogram">
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
  <DiagramLayerSettings obstacle="0" zIndex="0" showAll="1" priority="0" dist="0" placement="0" linePlacementFlags="18">
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
    <field name="loc_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="loc_code" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="loc_reference_level" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="loc_bank_level" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="loc_friction_type" configurationFlags="None">
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
    <field name="loc_friction_value" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="loc_definition_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="loc_channel_id" configurationFlags="None">
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
  </fieldConfiguration>
  <aliases>
    <alias name="" index="0" field="ROWID"/>
    <alias name="id" index="1" field="loc_id"/>
    <alias name="code" index="2" field="loc_code"/>
    <alias name="reference_level" index="3" field="loc_reference_level"/>
    <alias name="bank_level" index="4" field="loc_bank_level"/>
    <alias name="friction_type" index="5" field="loc_friction_type"/>
    <alias name="friction_value" index="6" field="loc_friction_value"/>
    <alias name="definition_id" index="7" field="loc_definition_id"/>
    <alias name="channel_id" index="8" field="loc_channel_id"/>
    <alias name="" index="9" field="def_id"/>
    <alias name="" index="10" field="def_shape"/>
    <alias name="" index="11" field="def_width"/>
    <alias name="" index="12" field="def_code"/>
    <alias name="" index="13" field="def_height"/>
  </aliases>
  <defaults>
    <default expression="" applyOnUpdate="0" field="ROWID"/>
    <default expression="if(maximum(loc_id) is null,1,maximum(loc_id)+1)" applyOnUpdate="0" field="loc_id"/>
    <default expression="'new'" applyOnUpdate="0" field="loc_code"/>
    <default expression="" applyOnUpdate="0" field="loc_reference_level"/>
    <default expression="" applyOnUpdate="0" field="loc_bank_level"/>
    <default expression="2" applyOnUpdate="0" field="loc_friction_type"/>
    <default expression="" applyOnUpdate="0" field="loc_friction_value"/>
    <default expression="" applyOnUpdate="0" field="loc_definition_id"/>
    <default expression="aggregate('v2_channel','min',&quot;id&quot;, intersects($geometry,geometry(@parent)))" applyOnUpdate="0" field="loc_channel_id"/>
    <default expression="" applyOnUpdate="0" field="def_id"/>
    <default expression="" applyOnUpdate="0" field="def_shape"/>
    <default expression="" applyOnUpdate="0" field="def_width"/>
    <default expression="" applyOnUpdate="0" field="def_code"/>
    <default expression="" applyOnUpdate="0" field="def_height"/>
  </defaults>
  <constraints>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="ROWID"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="loc_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="loc_code"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="loc_reference_level"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="loc_bank_level"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="loc_friction_type"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="loc_friction_value"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="loc_definition_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="loc_channel_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="def_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="def_shape"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="def_width"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="def_code"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="def_height"/>
  </constraints>
  <constraintExpressions>
    <constraint desc="" exp="" field="ROWID"/>
    <constraint desc="" exp="" field="loc_id"/>
    <constraint desc="" exp="" field="loc_code"/>
    <constraint desc="" exp="" field="loc_reference_level"/>
    <constraint desc="" exp="" field="loc_bank_level"/>
    <constraint desc="" exp="" field="loc_friction_type"/>
    <constraint desc="" exp="" field="loc_friction_value"/>
    <constraint desc="" exp="" field="loc_definition_id"/>
    <constraint desc="" exp="" field="loc_channel_id"/>
    <constraint desc="" exp="" field="def_id"/>
    <constraint desc="" exp="" field="def_shape"/>
    <constraint desc="" exp="" field="def_width"/>
    <constraint desc="" exp="" field="def_code"/>
    <constraint desc="" exp="" field="def_height"/>
  </constraintExpressions>
  <expressionfields/>
  <attributeactions>
    <defaultAction key="Canvas" value="{00000000-0000-0000-0000-000000000000}"/>
  </attributeactions>
  <attributetableconfig actionWidgetStyle="dropDown" sortExpression="" sortOrder="0">
    <columns>
      <column width="-1" type="actions" hidden="1"/>
      <column width="-1" type="field" name="def_id" hidden="0"/>
      <column width="-1" type="field" name="def_shape" hidden="0"/>
      <column width="-1" type="field" name="def_width" hidden="0"/>
      <column width="-1" type="field" name="def_code" hidden="0"/>
      <column width="-1" type="field" name="def_height" hidden="0"/>
      <column width="-1" type="field" name="ROWID" hidden="0"/>
      <column width="-1" type="field" name="loc_id" hidden="0"/>
      <column width="-1" type="field" name="loc_code" hidden="0"/>
      <column width="-1" type="field" name="loc_reference_level" hidden="0"/>
      <column width="-1" type="field" name="loc_bank_level" hidden="0"/>
      <column width="-1" type="field" name="loc_friction_type" hidden="0"/>
      <column width="-1" type="field" name="loc_friction_value" hidden="0"/>
      <column width="-1" type="field" name="loc_definition_id" hidden="0"/>
      <column width="-1" type="field" name="loc_channel_id" hidden="0"/>
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
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Cross section location view" visibilityExpression="" groupBox="0" showLabel="1">
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="General" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="loc_id" showLabel="1" index="1"/>
        <attributeEditorField name="loc_code" showLabel="1" index="2"/>
        <attributeEditorField name="loc_reference_level" showLabel="1" index="3"/>
        <attributeEditorField name="loc_bank_level" showLabel="1" index="4"/>
        <attributeEditorField name="loc_friction_type" showLabel="1" index="5"/>
        <attributeEditorField name="loc_friction_value" showLabel="1" index="6"/>
        <attributeEditorField name="loc_channel_id" showLabel="1" index="8"/>
      </attributeEditorContainer>
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Cross section" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="loc_definition_id" showLabel="1" index="7"/>
        <attributeEditorField name="def_code" showLabel="1" index="12"/>
        <attributeEditorField name="def_shape" showLabel="1" index="10"/>
        <attributeEditorField name="def_width" showLabel="1" index="11"/>
        <attributeEditorField name="def_height" showLabel="1" index="13"/>
      </attributeEditorContainer>
    </attributeEditorContainer>
  </attributeEditorForm>
  <editable>
    <field name="ROWID" editable="1"/>
    <field name="bank_level" editable="1"/>
    <field name="channel_id" editable="1"/>
    <field name="code" editable="1"/>
    <field name="def_code" editable="0"/>
    <field name="def_height" editable="0"/>
    <field name="def_id" editable="1"/>
    <field name="def_shape" editable="0"/>
    <field name="def_width" editable="0"/>
    <field name="definition_id" editable="1"/>
    <field name="friction_type" editable="1"/>
    <field name="friction_value" editable="1"/>
    <field name="id" editable="1"/>
    <field name="loc_bank_level" editable="1"/>
    <field name="loc_channel_id" editable="1"/>
    <field name="loc_code" editable="1"/>
    <field name="loc_definition_id" editable="1"/>
    <field name="loc_friction_type" editable="1"/>
    <field name="loc_friction_value" editable="1"/>
    <field name="loc_id" editable="1"/>
    <field name="loc_reference_level" editable="1"/>
    <field name="location_bank_level" editable="1"/>
    <field name="location_channel_id" editable="1"/>
    <field name="location_code" editable="1"/>
    <field name="location_definition_id" editable="1"/>
    <field name="location_friction_type" editable="1"/>
    <field name="location_friction_value" editable="1"/>
    <field name="location_id" editable="1"/>
    <field name="location_reference_level" editable="1"/>
    <field name="reference_level" editable="1"/>
    <field name="v2_cross_section_definition_code" editable="0"/>
    <field name="v2_cross_section_definition_height" editable="0"/>
    <field name="v2_cross_section_definition_shape" editable="0"/>
    <field name="v2_cross_section_definition_width" editable="0"/>
  </editable>
  <labelOnTop>
    <field name="ROWID" labelOnTop="0"/>
    <field name="bank_level" labelOnTop="0"/>
    <field name="channel_id" labelOnTop="0"/>
    <field name="code" labelOnTop="0"/>
    <field name="def_code" labelOnTop="0"/>
    <field name="def_height" labelOnTop="0"/>
    <field name="def_id" labelOnTop="0"/>
    <field name="def_shape" labelOnTop="0"/>
    <field name="def_width" labelOnTop="0"/>
    <field name="definition_id" labelOnTop="0"/>
    <field name="friction_type" labelOnTop="0"/>
    <field name="friction_value" labelOnTop="0"/>
    <field name="id" labelOnTop="0"/>
    <field name="loc_bank_level" labelOnTop="0"/>
    <field name="loc_channel_id" labelOnTop="0"/>
    <field name="loc_code" labelOnTop="0"/>
    <field name="loc_definition_id" labelOnTop="0"/>
    <field name="loc_friction_type" labelOnTop="0"/>
    <field name="loc_friction_value" labelOnTop="0"/>
    <field name="loc_id" labelOnTop="0"/>
    <field name="loc_reference_level" labelOnTop="0"/>
    <field name="location_bank_level" labelOnTop="0"/>
    <field name="location_channel_id" labelOnTop="0"/>
    <field name="location_code" labelOnTop="0"/>
    <field name="location_definition_id" labelOnTop="0"/>
    <field name="location_friction_type" labelOnTop="0"/>
    <field name="location_friction_value" labelOnTop="0"/>
    <field name="location_id" labelOnTop="0"/>
    <field name="location_reference_level" labelOnTop="0"/>
    <field name="reference_level" labelOnTop="0"/>
    <field name="v2_cross_section_definition_code" labelOnTop="0"/>
    <field name="v2_cross_section_definition_height" labelOnTop="0"/>
    <field name="v2_cross_section_definition_shape" labelOnTop="0"/>
    <field name="v2_cross_section_definition_width" labelOnTop="0"/>
  </labelOnTop>
  <dataDefinedFieldProperties/>
  <widgets/>
  <previewExpression>location_id</previewExpression>
  <mapTip></mapTip>
  <layerGeometryType>0</layerGeometryType>
</qgis>

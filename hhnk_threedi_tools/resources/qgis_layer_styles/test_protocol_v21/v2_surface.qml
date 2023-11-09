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
  <renderer-v2 symbollevels="0" forceraster="0" type="singleSymbol" enableorderby="0">
    <symbols>
      <symbol type="fill" name="0" alpha="1" clip_to_extent="1" force_rhr="0">
        <layer enabled="1" pass="0" class="SimpleFill" locked="0">
          <prop v="3x:0,0,0,0,0,0" k="border_width_map_unit_scale"/>
          <prop v="244,198,119,255" k="color"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="0,0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="133,112,8,255" k="outline_color"/>
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
    </symbols>
    <rotation/>
    <sizescale/>
  </renderer-v2>
  <customproperties>
    <property key="embeddedWidgets/count" value="0"/>
    <property key="variableNames"/>
    <property key="variableValues"/>
  </customproperties>
  <blendMode>0</blendMode>
  <featureBlendMode>0</featureBlendMode>
  <layerOpacity>0.9</layerOpacity>
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
    <field name="id" configurationFlags="None">
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
    <field name="zoom_category" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
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
    <field name="nr_of_inhabitants" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="dry_weather_flow" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="function" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="area" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="surface_parameters_id" configurationFlags="None">
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
    <alias name="" index="0" field="id"/>
    <alias name="" index="1" field="display_name"/>
    <alias name="" index="2" field="code"/>
    <alias name="" index="3" field="zoom_category"/>
    <alias name="" index="4" field="nr_of_inhabitants"/>
    <alias name="" index="5" field="dry_weather_flow"/>
    <alias name="" index="6" field="function"/>
    <alias name="" index="7" field="area"/>
    <alias name="" index="8" field="surface_parameters_id"/>
  </aliases>
  <defaults>
    <default expression="if(maximum(id) is null,1, maximum(id)+1)" applyOnUpdate="0" field="id"/>
    <default expression="'new'" applyOnUpdate="0" field="display_name"/>
    <default expression="'new'" applyOnUpdate="0" field="code"/>
    <default expression="-1" applyOnUpdate="0" field="zoom_category"/>
    <default expression="" applyOnUpdate="0" field="nr_of_inhabitants"/>
    <default expression="" applyOnUpdate="0" field="dry_weather_flow"/>
    <default expression="" applyOnUpdate="0" field="function"/>
    <default expression="$area" applyOnUpdate="0" field="area"/>
    <default expression="" applyOnUpdate="0" field="surface_parameters_id"/>
  </defaults>
  <constraints>
    <constraint unique_strength="1" exp_strength="0" notnull_strength="1" constraints="3" field="id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="display_name"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="code"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="zoom_category"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="nr_of_inhabitants"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="dry_weather_flow"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="function"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="area"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="surface_parameters_id"/>
  </constraints>
  <constraintExpressions>
    <constraint desc="" exp="" field="id"/>
    <constraint desc="" exp="" field="display_name"/>
    <constraint desc="" exp="" field="code"/>
    <constraint desc="" exp="" field="zoom_category"/>
    <constraint desc="" exp="" field="nr_of_inhabitants"/>
    <constraint desc="" exp="" field="dry_weather_flow"/>
    <constraint desc="" exp="" field="function"/>
    <constraint desc="" exp="" field="area"/>
    <constraint desc="" exp="" field="surface_parameters_id"/>
  </constraintExpressions>
  <expressionfields/>
  <attributeactions>
    <defaultAction key="Canvas" value="{00000000-0000-0000-0000-000000000000}"/>
  </attributeactions>
  <attributetableconfig actionWidgetStyle="dropDown" sortExpression="" sortOrder="0">
    <columns>
      <column width="-1" type="field" name="id" hidden="0"/>
      <column width="-1" type="field" name="display_name" hidden="0"/>
      <column width="-1" type="field" name="code" hidden="0"/>
      <column width="-1" type="field" name="zoom_category" hidden="0"/>
      <column width="-1" type="field" name="nr_of_inhabitants" hidden="0"/>
      <column width="-1" type="field" name="dry_weather_flow" hidden="0"/>
      <column width="-1" type="field" name="function" hidden="0"/>
      <column width="-1" type="field" name="area" hidden="0"/>
      <column width="-1" type="field" name="surface_parameters_id" hidden="0"/>
      <column width="-1" type="actions" hidden="1"/>
    </columns>
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
  <editforminitcode><![CDATA[# -*- coding: utf-8 -*-
"""
QGIS forms can have a Python function that is called when the form is
opened.

Use this function to add extra logic to your forms.

Enter the name of the function in the "Python Init function"
field.
An example follows:
"""
from qgis.PyQt.QtWidgets import QWidget

def my_form_open(dialog, layer, feature):
	geom = feature.geometry()
	control = dialog.findChild(QWidget, "MyLineEdit")
]]></editforminitcode>
  <featformsuppress>0</featformsuppress>
  <editorlayout>tablayout</editorlayout>
  <attributeEditorForm>
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Surface" visibilityExpression="" groupBox="0" showLabel="1">
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="General" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="id" showLabel="1" index="0"/>
        <attributeEditorField name="display_name" showLabel="1" index="1"/>
        <attributeEditorField name="code" showLabel="1" index="2"/>
      </attributeEditorContainer>
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Characteristics" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Rain water" visibilityExpression="" groupBox="1" showLabel="1">
          <attributeEditorField name="surface_parameters_id" showLabel="1" index="8"/>
          <attributeEditorField name="area" showLabel="1" index="7"/>
        </attributeEditorContainer>
        <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Municipal water" visibilityExpression="" groupBox="1" showLabel="1">
          <attributeEditorField name="nr_of_inhabitants" showLabel="1" index="4"/>
          <attributeEditorField name="dry_weather_flow" showLabel="1" index="5"/>
        </attributeEditorContainer>
      </attributeEditorContainer>
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Visualization" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="zoom_category" showLabel="1" index="3"/>
        <attributeEditorField name="function" showLabel="1" index="6"/>
      </attributeEditorContainer>
    </attributeEditorContainer>
  </attributeEditorForm>
  <editable>
    <field name="area" editable="1"/>
    <field name="code" editable="1"/>
    <field name="display_name" editable="1"/>
    <field name="dry_weather_flow" editable="1"/>
    <field name="function" editable="1"/>
    <field name="id" editable="1"/>
    <field name="nr_of_inhabitants" editable="1"/>
    <field name="surface_parameters_id" editable="1"/>
    <field name="zoom_category" editable="1"/>
  </editable>
  <labelOnTop>
    <field name="area" labelOnTop="0"/>
    <field name="code" labelOnTop="0"/>
    <field name="display_name" labelOnTop="0"/>
    <field name="dry_weather_flow" labelOnTop="0"/>
    <field name="function" labelOnTop="0"/>
    <field name="id" labelOnTop="0"/>
    <field name="nr_of_inhabitants" labelOnTop="0"/>
    <field name="surface_parameters_id" labelOnTop="0"/>
    <field name="zoom_category" labelOnTop="0"/>
  </labelOnTop>
  <dataDefinedFieldProperties/>
  <widgets/>
  <previewExpression>display_name</previewExpression>
  <mapTip></mapTip>
  <layerGeometryType>2</layerGeometryType>
</qgis>

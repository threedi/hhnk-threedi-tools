<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis readOnly="0" styleCategories="AllStyleCategories" hasScaleBasedVisibilityFlag="0" minScale="1e+08" version="3.16.11-Hannover" maxScale="0">
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
  <customproperties>
    <property key="dualview/previewExpressions" value="var_name"/>
    <property key="embeddedWidgets/count" value="0"/>
    <property key="variableNames"/>
    <property key="variableValues"/>
  </customproperties>
  <geometryOptions removeDuplicateNodes="0" geometryPrecision="0">
    <activeChecks/>
    <checkConfiguration/>
  </geometryOptions>
  <legend type="default-vector"/>
  <referencedLayers/>
  <fieldConfiguration>
    <field name="global_settings_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="timestep" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="var_name" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="aggregation_in_space" configurationFlags="None">
      <editWidget type="Hidden">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="aggregation_method" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="average" value="avg"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="minimum" value="min"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="maximum" value="max"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="cumulative" value="cum"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="median" value="med"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="cumulative negative" value="cum_negative"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="cumulative positive" value="cum_positive"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="current" value="current"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="flow_variable" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="discharge" value="discharge"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="flow velocity" value="flow_velocity"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="pump discharge" value="pump_discharge"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="rain" value="rain"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="waterlevel" value="waterlevel"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="wet cross section" value="wet_cross-section"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="wet surface" value="wet_surface"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="lateral discharge" value="lateral_discharge"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="volume" value="volume"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="infiltration" value="simple_infiltration"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="leakage" value="leakage"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="interception" value="interception"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
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
  </fieldConfiguration>
  <aliases>
    <alias name="" index="0" field="global_settings_id"/>
    <alias name="" index="1" field="timestep"/>
    <alias name="" index="2" field="var_name"/>
    <alias name="" index="3" field="aggregation_in_space"/>
    <alias name="" index="4" field="aggregation_method"/>
    <alias name="" index="5" field="flow_variable"/>
    <alias name="" index="6" field="id"/>
  </aliases>
  <defaults>
    <default expression="" applyOnUpdate="0" field="global_settings_id"/>
    <default expression="" applyOnUpdate="0" field="timestep"/>
    <default expression="" applyOnUpdate="0" field="var_name"/>
    <default expression="0" applyOnUpdate="0" field="aggregation_in_space"/>
    <default expression="" applyOnUpdate="0" field="aggregation_method"/>
    <default expression="" applyOnUpdate="0" field="flow_variable"/>
    <default expression="if(maximum(id) is null,1, maximum(id)+1)" applyOnUpdate="0" field="id"/>
  </defaults>
  <constraints>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="global_settings_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="timestep"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="var_name"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="aggregation_in_space"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="aggregation_method"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="flow_variable"/>
    <constraint unique_strength="1" exp_strength="0" notnull_strength="1" constraints="3" field="id"/>
  </constraints>
  <constraintExpressions>
    <constraint desc="" exp="" field="global_settings_id"/>
    <constraint desc="" exp="" field="timestep"/>
    <constraint desc="" exp="" field="var_name"/>
    <constraint desc="" exp="" field="aggregation_in_space"/>
    <constraint desc="" exp="" field="aggregation_method"/>
    <constraint desc="" exp="" field="flow_variable"/>
    <constraint desc="" exp="" field="id"/>
  </constraintExpressions>
  <expressionfields/>
  <attributeactions>
    <defaultAction key="Canvas" value="{00000000-0000-0000-0000-000000000000}"/>
  </attributeactions>
  <attributetableconfig actionWidgetStyle="dropDown" sortExpression="" sortOrder="0">
    <columns>
      <column width="214" type="field" name="timestep" hidden="0"/>
      <column width="207" type="field" name="var_name" hidden="0"/>
      <column width="123" type="field" name="global_settings_id" hidden="0"/>
      <column width="168" type="field" name="aggregation_in_space" hidden="0"/>
      <column width="-1" type="field" name="aggregation_method" hidden="0"/>
      <column width="-1" type="field" name="flow_variable" hidden="0"/>
      <column width="-1" type="field" name="id" hidden="0"/>
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
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="General" visibilityExpression="" groupBox="0" showLabel="1">
      <attributeEditorField name="id" showLabel="1" index="6"/>
      <attributeEditorField name="flow_variable" showLabel="1" index="5"/>
      <attributeEditorField name="aggregation_method" showLabel="1" index="4"/>
      <attributeEditorField name="timestep" showLabel="1" index="1"/>
      <attributeEditorField name="var_name" showLabel="1" index="2"/>
      <attributeEditorField name="global_settings_id" showLabel="1" index="0"/>
    </attributeEditorContainer>
  </attributeEditorForm>
  <editable>
    <field name="aggregation_in_space" editable="1"/>
    <field name="aggregation_method" editable="1"/>
    <field name="flow_variable" editable="1"/>
    <field name="global_settings_id" editable="1"/>
    <field name="id" editable="1"/>
    <field name="timestep" editable="1"/>
    <field name="var_name" editable="1"/>
  </editable>
  <labelOnTop>
    <field name="aggregation_in_space" labelOnTop="0"/>
    <field name="aggregation_method" labelOnTop="0"/>
    <field name="flow_variable" labelOnTop="0"/>
    <field name="global_settings_id" labelOnTop="0"/>
    <field name="id" labelOnTop="0"/>
    <field name="timestep" labelOnTop="0"/>
    <field name="var_name" labelOnTop="0"/>
  </labelOnTop>
  <dataDefinedFieldProperties/>
  <widgets/>
  <previewExpression>var_name</previewExpression>
  <mapTip></mapTip>
  <layerGeometryType>4</layerGeometryType>
</qgis>

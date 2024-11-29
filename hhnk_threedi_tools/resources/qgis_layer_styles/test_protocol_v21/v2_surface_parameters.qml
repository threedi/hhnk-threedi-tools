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
    <field name="outflow_delay" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="surface_layer_thickness" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="infiltration" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="max_infiltration_capacity" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="min_infiltration_capacity" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="infiltration_decay_constant" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="infiltration_recovery_constant" configurationFlags="None">
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
    <alias name="" index="1" field="outflow_delay"/>
    <alias name="" index="2" field="surface_layer_thickness"/>
    <alias name="" index="3" field="infiltration"/>
    <alias name="" index="4" field="max_infiltration_capacity"/>
    <alias name="" index="5" field="min_infiltration_capacity"/>
    <alias name="" index="6" field="infiltration_decay_constant"/>
    <alias name="" index="7" field="infiltration_recovery_constant"/>
  </aliases>
  <defaults>
    <default expression="if(maximum(id) is null,1, maximum(id)+1)" applyOnUpdate="0" field="id"/>
    <default expression="" applyOnUpdate="0" field="outflow_delay"/>
    <default expression="" applyOnUpdate="0" field="surface_layer_thickness"/>
    <default expression="" applyOnUpdate="0" field="infiltration"/>
    <default expression="" applyOnUpdate="0" field="max_infiltration_capacity"/>
    <default expression="" applyOnUpdate="0" field="min_infiltration_capacity"/>
    <default expression="" applyOnUpdate="0" field="infiltration_decay_constant"/>
    <default expression="" applyOnUpdate="0" field="infiltration_recovery_constant"/>
  </defaults>
  <constraints>
    <constraint unique_strength="1" exp_strength="0" notnull_strength="1" constraints="3" field="id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="outflow_delay"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="surface_layer_thickness"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="infiltration"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="max_infiltration_capacity"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="min_infiltration_capacity"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="infiltration_decay_constant"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="infiltration_recovery_constant"/>
  </constraints>
  <constraintExpressions>
    <constraint desc="" exp="" field="id"/>
    <constraint desc="" exp="" field="outflow_delay"/>
    <constraint desc="" exp="" field="surface_layer_thickness"/>
    <constraint desc="" exp="" field="infiltration"/>
    <constraint desc="" exp="" field="max_infiltration_capacity"/>
    <constraint desc="" exp="" field="min_infiltration_capacity"/>
    <constraint desc="" exp="" field="infiltration_decay_constant"/>
    <constraint desc="" exp="" field="infiltration_recovery_constant"/>
  </constraintExpressions>
  <expressionfields/>
  <attributeactions>
    <defaultAction key="Canvas" value="{00000000-0000-0000-0000-000000000000}"/>
  </attributeactions>
  <attributetableconfig actionWidgetStyle="dropDown" sortExpression="" sortOrder="0">
    <columns>
      <column width="-1" type="field" name="id" hidden="0"/>
      <column width="-1" type="field" name="outflow_delay" hidden="0"/>
      <column width="-1" type="field" name="surface_layer_thickness" hidden="0"/>
      <column width="-1" type="field" name="infiltration" hidden="0"/>
      <column width="-1" type="field" name="max_infiltration_capacity" hidden="0"/>
      <column width="-1" type="field" name="min_infiltration_capacity" hidden="0"/>
      <column width="-1" type="field" name="infiltration_decay_constant" hidden="0"/>
      <column width="-1" type="field" name="infiltration_recovery_constant" hidden="0"/>
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
      <attributeEditorField name="id" showLabel="1" index="0"/>
      <attributeEditorField name="infiltration" showLabel="1" index="3"/>
      <attributeEditorField name="max_infiltration_capacity" showLabel="1" index="4"/>
      <attributeEditorField name="min_infiltration_capacity" showLabel="1" index="5"/>
      <attributeEditorField name="infiltration_decay_constant" showLabel="1" index="6"/>
      <attributeEditorField name="infiltration_recovery_constant" showLabel="1" index="7"/>
      <attributeEditorField name="surface_layer_thickness" showLabel="1" index="2"/>
      <attributeEditorField name="outflow_delay" showLabel="1" index="1"/>
    </attributeEditorContainer>
  </attributeEditorForm>
  <editable>
    <field name="id" editable="1"/>
    <field name="infiltration" editable="1"/>
    <field name="infiltration_decay_constant" editable="1"/>
    <field name="infiltration_recovery_constant" editable="1"/>
    <field name="max_infiltration_capacity" editable="1"/>
    <field name="min_infiltration_capacity" editable="1"/>
    <field name="outflow_delay" editable="1"/>
    <field name="surface_layer_thickness" editable="1"/>
  </editable>
  <labelOnTop>
    <field name="id" labelOnTop="0"/>
    <field name="infiltration" labelOnTop="0"/>
    <field name="infiltration_decay_constant" labelOnTop="0"/>
    <field name="infiltration_recovery_constant" labelOnTop="0"/>
    <field name="max_infiltration_capacity" labelOnTop="0"/>
    <field name="min_infiltration_capacity" labelOnTop="0"/>
    <field name="outflow_delay" labelOnTop="0"/>
    <field name="surface_layer_thickness" labelOnTop="0"/>
  </labelOnTop>
  <dataDefinedFieldProperties/>
  <widgets/>
  <previewExpression>id</previewExpression>
  <mapTip></mapTip>
  <layerGeometryType>4</layerGeometryType>
</qgis>

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
    <property key="dualview/previewExpressions" value="id"/>
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
    <field name="interflow_type" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="0: No interflow" value="0"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="1: Porosity is rescaled per computational cell with respect to the deepest surface level in that cell. (Defining the porosity_layer_thickness is mandatory)" value="1"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="2: Porosity is rescaled per computational cell with respect to the deepest surface level in the 2D surface domain. (Defining the porosity_layer_thickness is mandatory)" value="2"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="3: The impervious layer thickness is uniform in the 2D surface domain and is based on the impervious_layer_elevation and the deepest surface level in that cell." value="3"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="4: The impervious layer thickness is non-uniform in the 2D surface domain and is based on the impervious_layer_elevation with respect to the deepest surface level in the 2D surface domain." value="4"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="porosity" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="porosity_file" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="porosity_layer_thickness" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="impervious_layer_elevation" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="hydraulic_conductivity" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="hydraulic_conductivity_file" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
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
  </fieldConfiguration>
  <aliases>
    <alias name="" index="0" field="id"/>
    <alias name="" index="1" field="interflow_type"/>
    <alias name="" index="2" field="porosity"/>
    <alias name="" index="3" field="porosity_file"/>
    <alias name="" index="4" field="porosity_layer_thickness"/>
    <alias name="" index="5" field="impervious_layer_elevation"/>
    <alias name="" index="6" field="hydraulic_conductivity"/>
    <alias name="" index="7" field="hydraulic_conductivity_file"/>
    <alias name="" index="8" field="display_name"/>
  </aliases>
  <defaults>
    <default expression="if(maximum(id) is null,1, maximum(id)+1)" applyOnUpdate="0" field="id"/>
    <default expression="" applyOnUpdate="0" field="interflow_type"/>
    <default expression="" applyOnUpdate="0" field="porosity"/>
    <default expression="" applyOnUpdate="0" field="porosity_file"/>
    <default expression="" applyOnUpdate="0" field="porosity_layer_thickness"/>
    <default expression="" applyOnUpdate="0" field="impervious_layer_elevation"/>
    <default expression="" applyOnUpdate="0" field="hydraulic_conductivity"/>
    <default expression="" applyOnUpdate="0" field="hydraulic_conductivity_file"/>
    <default expression="" applyOnUpdate="0" field="display_name"/>
  </defaults>
  <constraints>
    <constraint unique_strength="1" exp_strength="0" notnull_strength="1" constraints="3" field="id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="interflow_type"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="porosity"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="porosity_file"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="porosity_layer_thickness"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="impervious_layer_elevation"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="hydraulic_conductivity"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="hydraulic_conductivity_file"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="display_name"/>
  </constraints>
  <constraintExpressions>
    <constraint desc="" exp="" field="id"/>
    <constraint desc="" exp="" field="interflow_type"/>
    <constraint desc="" exp="" field="porosity"/>
    <constraint desc="" exp="" field="porosity_file"/>
    <constraint desc="" exp="" field="porosity_layer_thickness"/>
    <constraint desc="" exp="" field="impervious_layer_elevation"/>
    <constraint desc="" exp="" field="hydraulic_conductivity"/>
    <constraint desc="" exp="" field="hydraulic_conductivity_file"/>
    <constraint desc="" exp="" field="display_name"/>
  </constraintExpressions>
  <expressionfields/>
  <attributeactions>
    <defaultAction key="Canvas" value="{00000000-0000-0000-0000-000000000000}"/>
  </attributeactions>
  <attributetableconfig actionWidgetStyle="dropDown" sortExpression="" sortOrder="0">
    <columns>
      <column width="-1" type="field" name="id" hidden="0"/>
      <column width="-1" type="field" name="interflow_type" hidden="0"/>
      <column width="-1" type="field" name="porosity" hidden="0"/>
      <column width="-1" type="field" name="porosity_file" hidden="0"/>
      <column width="-1" type="field" name="porosity_layer_thickness" hidden="0"/>
      <column width="-1" type="field" name="impervious_layer_elevation" hidden="0"/>
      <column width="-1" type="field" name="hydraulic_conductivity" hidden="0"/>
      <column width="-1" type="field" name="hydraulic_conductivity_file" hidden="0"/>
      <column width="-1" type="field" name="display_name" hidden="0"/>
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
      <attributeEditorField name="display_name" showLabel="1" index="8"/>
      <attributeEditorField name="interflow_type" showLabel="1" index="1"/>
    </attributeEditorContainer>
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Porosity" visibilityExpression="" groupBox="0" showLabel="1">
      <attributeEditorField name="porosity" showLabel="1" index="2"/>
      <attributeEditorField name="porosity_file" showLabel="1" index="3"/>
      <attributeEditorField name="porosity_layer_thickness" showLabel="1" index="4"/>
    </attributeEditorContainer>
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Hydraulic conductivity" visibilityExpression="" groupBox="0" showLabel="1">
      <attributeEditorField name="hydraulic_conductivity_file" showLabel="1" index="7"/>
      <attributeEditorField name="hydraulic_conductivity" showLabel="1" index="6"/>
    </attributeEditorContainer>
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Impervious layer" visibilityExpression="" groupBox="0" showLabel="1">
      <attributeEditorField name="impervious_layer_elevation" showLabel="1" index="5"/>
    </attributeEditorContainer>
  </attributeEditorForm>
  <editable>
    <field name="display_name" editable="1"/>
    <field name="hydraulic_conductivity" editable="1"/>
    <field name="hydraulic_conductivity_file" editable="1"/>
    <field name="id" editable="1"/>
    <field name="impervious_layer_elevation" editable="1"/>
    <field name="interflow_type" editable="1"/>
    <field name="porosity" editable="1"/>
    <field name="porosity_file" editable="1"/>
    <field name="porosity_layer_thickness" editable="1"/>
  </editable>
  <labelOnTop>
    <field name="display_name" labelOnTop="0"/>
    <field name="hydraulic_conductivity" labelOnTop="0"/>
    <field name="hydraulic_conductivity_file" labelOnTop="0"/>
    <field name="id" labelOnTop="0"/>
    <field name="impervious_layer_elevation" labelOnTop="0"/>
    <field name="interflow_type" labelOnTop="0"/>
    <field name="porosity" labelOnTop="0"/>
    <field name="porosity_file" labelOnTop="0"/>
    <field name="porosity_layer_thickness" labelOnTop="0"/>
  </labelOnTop>
  <dataDefinedFieldProperties/>
  <widgets/>
  <previewExpression>id</previewExpression>
  <mapTip></mapTip>
  <layerGeometryType>4</layerGeometryType>
</qgis>

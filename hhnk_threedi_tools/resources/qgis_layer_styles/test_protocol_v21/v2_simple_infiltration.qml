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
    <field name="infiltration_rate" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="infiltration_rate_file" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="infiltration_surface_option" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="max_infiltration_volume_file" configurationFlags="None">
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
  </fieldConfiguration>
  <aliases>
    <alias name="" index="0" field="id"/>
    <alias name="" index="1" field="infiltration_rate"/>
    <alias name="" index="2" field="infiltration_rate_file"/>
    <alias name="" index="3" field="infiltration_surface_option"/>
    <alias name="" index="4" field="max_infiltration_volume_file"/>
    <alias name="" index="5" field="display_name"/>
  </aliases>
  <defaults>
    <default expression="if(maximum(id) is null,1,maximum(id)+1)" applyOnUpdate="0" field="id"/>
    <default expression="" applyOnUpdate="0" field="infiltration_rate"/>
    <default expression="" applyOnUpdate="0" field="infiltration_rate_file"/>
    <default expression="0" applyOnUpdate="0" field="infiltration_surface_option"/>
    <default expression="" applyOnUpdate="0" field="max_infiltration_volume_file"/>
    <default expression="'new'" applyOnUpdate="0" field="display_name"/>
  </defaults>
  <constraints>
    <constraint unique_strength="1" exp_strength="0" notnull_strength="1" constraints="3" field="id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="infiltration_rate"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="infiltration_rate_file"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="infiltration_surface_option"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="max_infiltration_volume_file"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="display_name"/>
  </constraints>
  <constraintExpressions>
    <constraint desc="" exp="" field="id"/>
    <constraint desc="" exp="" field="infiltration_rate"/>
    <constraint desc="" exp="" field="infiltration_rate_file"/>
    <constraint desc="" exp="" field="infiltration_surface_option"/>
    <constraint desc="" exp="" field="max_infiltration_volume_file"/>
    <constraint desc="" exp="" field="display_name"/>
  </constraintExpressions>
  <expressionfields/>
  <attributeactions>
    <defaultAction key="Canvas" value="{00000000-0000-0000-0000-000000000000}"/>
  </attributeactions>
  <attributetableconfig actionWidgetStyle="dropDown" sortExpression="" sortOrder="0">
    <columns>
      <column width="-1" type="field" name="id" hidden="0"/>
      <column width="-1" type="field" name="infiltration_rate" hidden="0"/>
      <column width="-1" type="field" name="infiltration_rate_file" hidden="0"/>
      <column width="-1" type="field" name="infiltration_surface_option" hidden="0"/>
      <column width="-1" type="field" name="max_infiltration_volume_file" hidden="0"/>
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
      <attributeEditorField name="display_name" showLabel="1" index="5"/>
      <attributeEditorField name="infiltration_rate" showLabel="1" index="1"/>
      <attributeEditorField name="infiltration_rate_file" showLabel="1" index="2"/>
      <attributeEditorField name="max_infiltration_volume_file" showLabel="1" index="4"/>
      <attributeEditorField name="infiltration_surface_option" showLabel="1" index="3"/>
    </attributeEditorContainer>
  </attributeEditorForm>
  <editable>
    <field name="display_name" editable="1"/>
    <field name="id" editable="1"/>
    <field name="infiltration_rate" editable="1"/>
    <field name="infiltration_rate_file" editable="1"/>
    <field name="infiltration_surface_option" editable="1"/>
    <field name="max_infiltration_volume_file" editable="1"/>
  </editable>
  <labelOnTop>
    <field name="display_name" labelOnTop="0"/>
    <field name="id" labelOnTop="0"/>
    <field name="infiltration_rate" labelOnTop="0"/>
    <field name="infiltration_rate_file" labelOnTop="0"/>
    <field name="infiltration_surface_option" labelOnTop="0"/>
    <field name="max_infiltration_volume_file" labelOnTop="0"/>
  </labelOnTop>
  <dataDefinedFieldProperties/>
  <widgets/>
  <previewExpression>id</previewExpression>
  <mapTip></mapTip>
  <layerGeometryType>4</layerGeometryType>
</qgis>

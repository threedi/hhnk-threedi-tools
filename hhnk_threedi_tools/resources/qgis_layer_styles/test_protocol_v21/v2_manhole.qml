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
    <field name="shape" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="00: square" value="00"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="01: round" value="01"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="02: rectangle" value="02"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="width" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="length" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="manhole_indicator" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="0: inspection" value="0"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="1: outlet" value="1"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="2: pumpstation" value="2"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="calculation_type" configurationFlags="None">
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
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="bottom_level" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="surface_level" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="drain_level" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="sediment_level" configurationFlags="None">
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
  </fieldConfiguration>
  <aliases>
    <alias name="" index="0" field="id"/>
    <alias name="" index="1" field="display_name"/>
    <alias name="" index="2" field="code"/>
    <alias name="" index="3" field="connection_node_id"/>
    <alias name="" index="4" field="shape"/>
    <alias name="" index="5" field="width"/>
    <alias name="" index="6" field="length"/>
    <alias name="" index="7" field="manhole_indicator"/>
    <alias name="" index="8" field="calculation_type"/>
    <alias name="" index="9" field="bottom_level"/>
    <alias name="" index="10" field="surface_level"/>
    <alias name="" index="11" field="drain_level"/>
    <alias name="" index="12" field="sediment_level"/>
    <alias name="" index="13" field="zoom_category"/>
  </aliases>
  <defaults>
    <default expression="if(maximum(id) is null,1, maximum(id)+1)" applyOnUpdate="0" field="id"/>
    <default expression="'new'" applyOnUpdate="0" field="display_name"/>
    <default expression="'new'" applyOnUpdate="0" field="code"/>
    <default expression="" applyOnUpdate="0" field="connection_node_id"/>
    <default expression="" applyOnUpdate="0" field="shape"/>
    <default expression="" applyOnUpdate="0" field="width"/>
    <default expression="" applyOnUpdate="0" field="length"/>
    <default expression="0" applyOnUpdate="0" field="manhole_indicator"/>
    <default expression="" applyOnUpdate="0" field="calculation_type"/>
    <default expression="" applyOnUpdate="0" field="bottom_level"/>
    <default expression="" applyOnUpdate="0" field="surface_level"/>
    <default expression="" applyOnUpdate="0" field="drain_level"/>
    <default expression="" applyOnUpdate="0" field="sediment_level"/>
    <default expression="1" applyOnUpdate="0" field="zoom_category"/>
  </defaults>
  <constraints>
    <constraint unique_strength="1" exp_strength="0" notnull_strength="1" constraints="3" field="id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="1" constraints="1" field="display_name"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="1" constraints="1" field="code"/>
    <constraint unique_strength="1" exp_strength="0" notnull_strength="2" constraints="3" field="connection_node_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="shape"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="width"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="length"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="manhole_indicator"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="calculation_type"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="bottom_level"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="surface_level"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="drain_level"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="sediment_level"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="zoom_category"/>
  </constraints>
  <constraintExpressions>
    <constraint desc="" exp="" field="id"/>
    <constraint desc="" exp="" field="display_name"/>
    <constraint desc="" exp="" field="code"/>
    <constraint desc="" exp="" field="connection_node_id"/>
    <constraint desc="" exp="" field="shape"/>
    <constraint desc="" exp="" field="width"/>
    <constraint desc="" exp="" field="length"/>
    <constraint desc="" exp="" field="manhole_indicator"/>
    <constraint desc="" exp="" field="calculation_type"/>
    <constraint desc="" exp="" field="bottom_level"/>
    <constraint desc="" exp="" field="surface_level"/>
    <constraint desc="" exp="" field="drain_level"/>
    <constraint desc="" exp="" field="sediment_level"/>
    <constraint desc="" exp="" field="zoom_category"/>
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
      <column width="-1" type="field" name="connection_node_id" hidden="0"/>
      <column width="-1" type="field" name="shape" hidden="0"/>
      <column width="-1" type="field" name="width" hidden="0"/>
      <column width="-1" type="field" name="length" hidden="0"/>
      <column width="-1" type="field" name="manhole_indicator" hidden="0"/>
      <column width="-1" type="field" name="calculation_type" hidden="0"/>
      <column width="-1" type="field" name="bottom_level" hidden="0"/>
      <column width="-1" type="field" name="surface_level" hidden="0"/>
      <column width="-1" type="field" name="drain_level" hidden="0"/>
      <column width="-1" type="field" name="sediment_level" hidden="0"/>
      <column width="-1" type="field" name="zoom_category" hidden="0"/>
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
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Manhole" visibilityExpression="" groupBox="0" showLabel="1">
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="General" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="id" showLabel="1" index="0"/>
        <attributeEditorField name="display_name" showLabel="1" index="1"/>
        <attributeEditorField name="code" showLabel="1" index="2"/>
        <attributeEditorField name="connection_node_id" showLabel="1" index="3"/>
        <attributeEditorField name="calculation_type" showLabel="1" index="8"/>
      </attributeEditorContainer>
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Characteristics" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="shape" showLabel="1" index="4"/>
        <attributeEditorField name="width" showLabel="1" index="5"/>
        <attributeEditorField name="length" showLabel="1" index="6"/>
        <attributeEditorField name="surface_level" showLabel="1" index="10"/>
        <attributeEditorField name="drain_level" showLabel="1" index="11"/>
        <attributeEditorField name="bottom_level" showLabel="1" index="9"/>
      </attributeEditorContainer>
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Visualization" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="zoom_category" showLabel="1" index="13"/>
        <attributeEditorField name="manhole_indicator" showLabel="1" index="7"/>
      </attributeEditorContainer>
    </attributeEditorContainer>
  </attributeEditorForm>
  <editable>
    <field name="bottom_level" editable="1"/>
    <field name="calculation_type" editable="1"/>
    <field name="code" editable="1"/>
    <field name="connection_node_id" editable="1"/>
    <field name="display_name" editable="1"/>
    <field name="drain_level" editable="1"/>
    <field name="id" editable="1"/>
    <field name="length" editable="1"/>
    <field name="manhole_indicator" editable="1"/>
    <field name="sediment_level" editable="1"/>
    <field name="shape" editable="1"/>
    <field name="surface_level" editable="1"/>
    <field name="width" editable="1"/>
    <field name="zoom_category" editable="1"/>
  </editable>
  <labelOnTop>
    <field name="bottom_level" labelOnTop="0"/>
    <field name="calculation_type" labelOnTop="0"/>
    <field name="code" labelOnTop="0"/>
    <field name="connection_node_id" labelOnTop="0"/>
    <field name="display_name" labelOnTop="0"/>
    <field name="drain_level" labelOnTop="0"/>
    <field name="id" labelOnTop="0"/>
    <field name="length" labelOnTop="0"/>
    <field name="manhole_indicator" labelOnTop="0"/>
    <field name="sediment_level" labelOnTop="0"/>
    <field name="shape" labelOnTop="0"/>
    <field name="surface_level" labelOnTop="0"/>
    <field name="width" labelOnTop="0"/>
    <field name="zoom_category" labelOnTop="0"/>
  </labelOnTop>
  <dataDefinedFieldProperties/>
  <widgets/>
  <previewExpression>id</previewExpression>
  <mapTip></mapTip>
  <layerGeometryType>4</layerGeometryType>
</qgis>

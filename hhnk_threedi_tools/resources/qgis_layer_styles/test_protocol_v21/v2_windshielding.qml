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
    <field name="channel_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="north" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="northeast" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="east" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="southeast" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="south" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="southwest" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="west" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="northwest" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="the_geom" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
  </fieldConfiguration>
  <aliases>
    <alias name="" index="0" field="id"/>
    <alias name="" index="1" field="channel_id"/>
    <alias name="" index="2" field="north"/>
    <alias name="" index="3" field="northeast"/>
    <alias name="" index="4" field="east"/>
    <alias name="" index="5" field="southeast"/>
    <alias name="" index="6" field="south"/>
    <alias name="" index="7" field="southwest"/>
    <alias name="" index="8" field="west"/>
    <alias name="" index="9" field="northwest"/>
    <alias name="" index="10" field="the_geom"/>
  </aliases>
  <defaults>
    <default expression="if(maximum(id) is null,1, maximum(id)+1)" applyOnUpdate="0" field="id"/>
    <default expression="" applyOnUpdate="0" field="channel_id"/>
    <default expression="" applyOnUpdate="0" field="north"/>
    <default expression="" applyOnUpdate="0" field="northeast"/>
    <default expression="" applyOnUpdate="0" field="east"/>
    <default expression="" applyOnUpdate="0" field="southeast"/>
    <default expression="" applyOnUpdate="0" field="south"/>
    <default expression="" applyOnUpdate="0" field="southwest"/>
    <default expression="" applyOnUpdate="0" field="west"/>
    <default expression="" applyOnUpdate="0" field="northwest"/>
    <default expression="" applyOnUpdate="0" field="the_geom"/>
  </defaults>
  <constraints>
    <constraint unique_strength="1" exp_strength="0" notnull_strength="1" constraints="3" field="id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="channel_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="north"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="northeast"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="east"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="southeast"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="south"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="southwest"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="west"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="northwest"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="the_geom"/>
  </constraints>
  <constraintExpressions>
    <constraint desc="" exp="" field="id"/>
    <constraint desc="" exp="" field="channel_id"/>
    <constraint desc="" exp="" field="north"/>
    <constraint desc="" exp="" field="northeast"/>
    <constraint desc="" exp="" field="east"/>
    <constraint desc="" exp="" field="southeast"/>
    <constraint desc="" exp="" field="south"/>
    <constraint desc="" exp="" field="southwest"/>
    <constraint desc="" exp="" field="west"/>
    <constraint desc="" exp="" field="northwest"/>
    <constraint desc="" exp="" field="the_geom"/>
  </constraintExpressions>
  <expressionfields/>
  <attributeactions>
    <defaultAction key="Canvas" value="{00000000-0000-0000-0000-000000000000}"/>
  </attributeactions>
  <attributetableconfig actionWidgetStyle="dropDown" sortExpression="" sortOrder="0">
    <columns>
      <column width="-1" type="field" name="id" hidden="0"/>
      <column width="-1" type="field" name="channel_id" hidden="0"/>
      <column width="-1" type="field" name="north" hidden="0"/>
      <column width="-1" type="field" name="northeast" hidden="0"/>
      <column width="-1" type="field" name="east" hidden="0"/>
      <column width="-1" type="field" name="southeast" hidden="0"/>
      <column width="-1" type="field" name="south" hidden="0"/>
      <column width="-1" type="field" name="southwest" hidden="0"/>
      <column width="-1" type="field" name="west" hidden="0"/>
      <column width="-1" type="field" name="northwest" hidden="0"/>
      <column width="-1" type="field" name="the_geom" hidden="0"/>
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
      <attributeEditorField name="channel_id" showLabel="1" index="1"/>
      <attributeEditorField name="north" showLabel="1" index="2"/>
      <attributeEditorField name="northeast" showLabel="1" index="3"/>
      <attributeEditorField name="east" showLabel="1" index="4"/>
      <attributeEditorField name="southeast" showLabel="1" index="5"/>
      <attributeEditorField name="south" showLabel="1" index="6"/>
      <attributeEditorField name="southwest" showLabel="1" index="7"/>
      <attributeEditorField name="west" showLabel="1" index="8"/>
      <attributeEditorField name="northwest" showLabel="1" index="9"/>
      <attributeEditorField name="the_geom" showLabel="1" index="10"/>
    </attributeEditorContainer>
  </attributeEditorForm>
  <editable>
    <field name="channel_id" editable="1"/>
    <field name="east" editable="1"/>
    <field name="id" editable="1"/>
    <field name="north" editable="1"/>
    <field name="northeast" editable="1"/>
    <field name="northwest" editable="1"/>
    <field name="south" editable="1"/>
    <field name="southeast" editable="1"/>
    <field name="southwest" editable="1"/>
    <field name="the_geom" editable="1"/>
    <field name="west" editable="1"/>
  </editable>
  <labelOnTop>
    <field name="channel_id" labelOnTop="0"/>
    <field name="east" labelOnTop="0"/>
    <field name="id" labelOnTop="0"/>
    <field name="north" labelOnTop="0"/>
    <field name="northeast" labelOnTop="0"/>
    <field name="northwest" labelOnTop="0"/>
    <field name="south" labelOnTop="0"/>
    <field name="southeast" labelOnTop="0"/>
    <field name="southwest" labelOnTop="0"/>
    <field name="the_geom" labelOnTop="0"/>
    <field name="west" labelOnTop="0"/>
  </labelOnTop>
  <dataDefinedFieldProperties/>
  <widgets/>
  <previewExpression>id</previewExpression>
  <mapTip></mapTip>
  <layerGeometryType>4</layerGeometryType>
</qgis>

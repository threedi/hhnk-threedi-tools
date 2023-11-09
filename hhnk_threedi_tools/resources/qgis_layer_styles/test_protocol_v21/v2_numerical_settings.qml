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
    <field name="cfl_strictness_factor_1d" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="cfl_strictness_factor_2d" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="convergence_cg" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="convergence_eps" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="flow_direction_threshold" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="frict_shallow_water_correction" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="general_numerical_threshold" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="integration_method" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="limiter_grad_1d" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="limiter_grad_2d" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="limiter_slope_crossectional_area_2d" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="limiter_slope_friction_2d" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="max_nonlin_iterations" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="max_degree" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="5: for surface 2D flow only" value="5"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="7: for 1D and 2D flow" value="7"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="70: for surface 1D, 2D surface and groundwater flow or higher" value="70"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="700: for 1D flow" value="700"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="minimum_friction_velocity" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="minimum_surface_area" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="precon_cg" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="preissmann_slot" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="pump_implicit_ratio" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="thin_water_layer_definition" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="use_of_cg" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="use_of_nested_newton" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="0: When the schematisation does not include 1D-elements with closed-profiles" value="0"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="1: When the schematisation  includes 1D-elements with closed-profiles" value="1"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
  </fieldConfiguration>
  <aliases>
    <alias name="" index="0" field="id"/>
    <alias name="" index="1" field="cfl_strictness_factor_1d"/>
    <alias name="" index="2" field="cfl_strictness_factor_2d"/>
    <alias name="" index="3" field="convergence_cg"/>
    <alias name="" index="4" field="convergence_eps"/>
    <alias name="" index="5" field="flow_direction_threshold"/>
    <alias name="" index="6" field="frict_shallow_water_correction"/>
    <alias name="" index="7" field="general_numerical_threshold"/>
    <alias name="" index="8" field="integration_method"/>
    <alias name="" index="9" field="limiter_grad_1d"/>
    <alias name="" index="10" field="limiter_grad_2d"/>
    <alias name="" index="11" field="limiter_slope_crossectional_area_2d"/>
    <alias name="" index="12" field="limiter_slope_friction_2d"/>
    <alias name="" index="13" field="max_nonlin_iterations"/>
    <alias name="" index="14" field="max_degree"/>
    <alias name="" index="15" field="minimum_friction_velocity"/>
    <alias name="" index="16" field="minimum_surface_area"/>
    <alias name="" index="17" field="precon_cg"/>
    <alias name="" index="18" field="preissmann_slot"/>
    <alias name="" index="19" field="pump_implicit_ratio"/>
    <alias name="" index="20" field="thin_water_layer_definition"/>
    <alias name="" index="21" field="use_of_cg"/>
    <alias name="" index="22" field="use_of_nested_newton"/>
  </aliases>
  <defaults>
    <default expression=" if(maximum(id) is null,1, maximum(id)+1)" applyOnUpdate="0" field="id"/>
    <default expression="1" applyOnUpdate="0" field="cfl_strictness_factor_1d"/>
    <default expression="1" applyOnUpdate="0" field="cfl_strictness_factor_2d"/>
    <default expression="0.000000001" applyOnUpdate="0" field="convergence_cg"/>
    <default expression="0.00001" applyOnUpdate="0" field="convergence_eps"/>
    <default expression="0.000001" applyOnUpdate="0" field="flow_direction_threshold"/>
    <default expression="0" applyOnUpdate="0" field="frict_shallow_water_correction"/>
    <default expression="0.00000001" applyOnUpdate="0" field="general_numerical_threshold"/>
    <default expression="0" applyOnUpdate="0" field="integration_method"/>
    <default expression="1" applyOnUpdate="0" field="limiter_grad_1d"/>
    <default expression="0" applyOnUpdate="0" field="limiter_grad_2d"/>
    <default expression="0" applyOnUpdate="0" field="limiter_slope_crossectional_area_2d"/>
    <default expression="0" applyOnUpdate="0" field="limiter_slope_friction_2d"/>
    <default expression="20" applyOnUpdate="0" field="max_nonlin_iterations"/>
    <default expression="" applyOnUpdate="0" field="max_degree"/>
    <default expression="0.05" applyOnUpdate="0" field="minimum_friction_velocity"/>
    <default expression="0.00000001" applyOnUpdate="0" field="minimum_surface_area"/>
    <default expression="1" applyOnUpdate="0" field="precon_cg"/>
    <default expression="0" applyOnUpdate="0" field="preissmann_slot"/>
    <default expression="1" applyOnUpdate="0" field="pump_implicit_ratio"/>
    <default expression="0.05" applyOnUpdate="0" field="thin_water_layer_definition"/>
    <default expression="20" applyOnUpdate="0" field="use_of_cg"/>
    <default expression="" applyOnUpdate="0" field="use_of_nested_newton"/>
  </defaults>
  <constraints>
    <constraint unique_strength="1" exp_strength="0" notnull_strength="1" constraints="3" field="id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="cfl_strictness_factor_1d"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="cfl_strictness_factor_2d"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="convergence_cg"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="convergence_eps"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="flow_direction_threshold"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="frict_shallow_water_correction"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="general_numerical_threshold"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="integration_method"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="limiter_grad_1d"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="limiter_grad_2d"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="limiter_slope_crossectional_area_2d"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="limiter_slope_friction_2d"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="max_nonlin_iterations"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="max_degree"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="minimum_friction_velocity"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="minimum_surface_area"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="precon_cg"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="preissmann_slot"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="pump_implicit_ratio"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="thin_water_layer_definition"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="use_of_cg"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="use_of_nested_newton"/>
  </constraints>
  <constraintExpressions>
    <constraint desc="" exp="" field="id"/>
    <constraint desc="" exp="" field="cfl_strictness_factor_1d"/>
    <constraint desc="" exp="" field="cfl_strictness_factor_2d"/>
    <constraint desc="" exp="" field="convergence_cg"/>
    <constraint desc="" exp="" field="convergence_eps"/>
    <constraint desc="" exp="" field="flow_direction_threshold"/>
    <constraint desc="" exp="" field="frict_shallow_water_correction"/>
    <constraint desc="" exp="" field="general_numerical_threshold"/>
    <constraint desc="" exp="" field="integration_method"/>
    <constraint desc="" exp="" field="limiter_grad_1d"/>
    <constraint desc="" exp="" field="limiter_grad_2d"/>
    <constraint desc="" exp="" field="limiter_slope_crossectional_area_2d"/>
    <constraint desc="" exp="" field="limiter_slope_friction_2d"/>
    <constraint desc="" exp="" field="max_nonlin_iterations"/>
    <constraint desc="" exp="" field="max_degree"/>
    <constraint desc="" exp="" field="minimum_friction_velocity"/>
    <constraint desc="" exp="" field="minimum_surface_area"/>
    <constraint desc="" exp="" field="precon_cg"/>
    <constraint desc="" exp="" field="preissmann_slot"/>
    <constraint desc="" exp="" field="pump_implicit_ratio"/>
    <constraint desc="" exp="" field="thin_water_layer_definition"/>
    <constraint desc="" exp="" field="use_of_cg"/>
    <constraint desc="" exp="" field="use_of_nested_newton"/>
  </constraintExpressions>
  <expressionfields/>
  <attributeactions>
    <defaultAction key="Canvas" value="{00000000-0000-0000-0000-000000000000}"/>
  </attributeactions>
  <attributetableconfig actionWidgetStyle="dropDown" sortExpression="" sortOrder="0">
    <columns>
      <column width="-1" type="field" name="id" hidden="0"/>
      <column width="-1" type="field" name="cfl_strictness_factor_1d" hidden="0"/>
      <column width="-1" type="field" name="cfl_strictness_factor_2d" hidden="0"/>
      <column width="-1" type="field" name="convergence_cg" hidden="0"/>
      <column width="-1" type="field" name="convergence_eps" hidden="0"/>
      <column width="-1" type="field" name="flow_direction_threshold" hidden="0"/>
      <column width="-1" type="field" name="frict_shallow_water_correction" hidden="0"/>
      <column width="-1" type="field" name="general_numerical_threshold" hidden="0"/>
      <column width="-1" type="field" name="integration_method" hidden="0"/>
      <column width="-1" type="field" name="limiter_grad_1d" hidden="0"/>
      <column width="-1" type="field" name="limiter_grad_2d" hidden="0"/>
      <column width="-1" type="field" name="limiter_slope_crossectional_area_2d" hidden="0"/>
      <column width="-1" type="field" name="limiter_slope_friction_2d" hidden="0"/>
      <column width="-1" type="field" name="max_nonlin_iterations" hidden="0"/>
      <column width="-1" type="field" name="max_degree" hidden="0"/>
      <column width="-1" type="field" name="minimum_friction_velocity" hidden="0"/>
      <column width="-1" type="field" name="minimum_surface_area" hidden="0"/>
      <column width="-1" type="field" name="precon_cg" hidden="0"/>
      <column width="-1" type="field" name="preissmann_slot" hidden="0"/>
      <column width="-1" type="field" name="pump_implicit_ratio" hidden="0"/>
      <column width="-1" type="field" name="thin_water_layer_definition" hidden="0"/>
      <column width="-1" type="field" name="use_of_cg" hidden="0"/>
      <column width="-1" type="field" name="use_of_nested_newton" hidden="0"/>
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
    </attributeEditorContainer>
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Limiters" visibilityExpression="" groupBox="0" showLabel="1">
      <attributeEditorField name="limiter_grad_1d" showLabel="1" index="9"/>
      <attributeEditorField name="limiter_grad_2d" showLabel="1" index="10"/>
      <attributeEditorField name="limiter_slope_crossectional_area_2d" showLabel="1" index="11"/>
      <attributeEditorField name="limiter_slope_friction_2d" showLabel="1" index="12"/>
    </attributeEditorContainer>
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Matrix" visibilityExpression="" groupBox="0" showLabel="1">
      <attributeEditorField name="convergence_cg" showLabel="1" index="3"/>
      <attributeEditorField name="convergence_eps" showLabel="1" index="4"/>
      <attributeEditorField name="use_of_cg" showLabel="1" index="21"/>
      <attributeEditorField name="use_of_nested_newton" showLabel="1" index="22"/>
      <attributeEditorField name="max_degree" showLabel="1" index="14"/>
      <attributeEditorField name="max_nonlin_iterations" showLabel="1" index="13"/>
      <attributeEditorField name="precon_cg" showLabel="1" index="17"/>
      <attributeEditorField name="integration_method" showLabel="1" index="8"/>
    </attributeEditorContainer>
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Thresholds" visibilityExpression="" groupBox="0" showLabel="1">
      <attributeEditorField name="flow_direction_threshold" showLabel="1" index="5"/>
      <attributeEditorField name="general_numerical_threshold" showLabel="1" index="7"/>
      <attributeEditorField name="thin_water_layer_definition" showLabel="1" index="20"/>
      <attributeEditorField name="minimum_friction_velocity" showLabel="1" index="15"/>
      <attributeEditorField name="minimum_surface_area" showLabel="1" index="16"/>
    </attributeEditorContainer>
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Miscellaneous" visibilityExpression="" groupBox="0" showLabel="1">
      <attributeEditorField name="cfl_strictness_factor_1d" showLabel="1" index="1"/>
      <attributeEditorField name="cfl_strictness_factor_2d" showLabel="1" index="2"/>
      <attributeEditorField name="frict_shallow_water_correction" showLabel="1" index="6"/>
      <attributeEditorField name="pump_implicit_ratio" showLabel="1" index="19"/>
      <attributeEditorField name="preissmann_slot" showLabel="1" index="18"/>
    </attributeEditorContainer>
  </attributeEditorForm>
  <editable>
    <field name="cfl_strictness_factor_1d" editable="1"/>
    <field name="cfl_strictness_factor_2d" editable="1"/>
    <field name="convergence_cg" editable="1"/>
    <field name="convergence_eps" editable="1"/>
    <field name="flow_direction_threshold" editable="1"/>
    <field name="frict_shallow_water_correction" editable="1"/>
    <field name="general_numerical_threshold" editable="1"/>
    <field name="id" editable="1"/>
    <field name="integration_method" editable="1"/>
    <field name="limiter_grad_1d" editable="1"/>
    <field name="limiter_grad_2d" editable="1"/>
    <field name="limiter_slope_crossectional_area_2d" editable="1"/>
    <field name="limiter_slope_friction_2d" editable="1"/>
    <field name="max_degree" editable="1"/>
    <field name="max_nonlin_iterations" editable="1"/>
    <field name="minimum_friction_velocity" editable="1"/>
    <field name="minimum_surface_area" editable="1"/>
    <field name="precon_cg" editable="1"/>
    <field name="preissmann_slot" editable="1"/>
    <field name="pump_implicit_ratio" editable="1"/>
    <field name="thin_water_layer_definition" editable="1"/>
    <field name="use_of_cg" editable="1"/>
    <field name="use_of_nested_newton" editable="1"/>
  </editable>
  <labelOnTop>
    <field name="cfl_strictness_factor_1d" labelOnTop="0"/>
    <field name="cfl_strictness_factor_2d" labelOnTop="0"/>
    <field name="convergence_cg" labelOnTop="0"/>
    <field name="convergence_eps" labelOnTop="0"/>
    <field name="flow_direction_threshold" labelOnTop="0"/>
    <field name="frict_shallow_water_correction" labelOnTop="0"/>
    <field name="general_numerical_threshold" labelOnTop="0"/>
    <field name="id" labelOnTop="0"/>
    <field name="integration_method" labelOnTop="0"/>
    <field name="limiter_grad_1d" labelOnTop="0"/>
    <field name="limiter_grad_2d" labelOnTop="0"/>
    <field name="limiter_slope_crossectional_area_2d" labelOnTop="0"/>
    <field name="limiter_slope_friction_2d" labelOnTop="0"/>
    <field name="max_degree" labelOnTop="0"/>
    <field name="max_nonlin_iterations" labelOnTop="0"/>
    <field name="minimum_friction_velocity" labelOnTop="0"/>
    <field name="minimum_surface_area" labelOnTop="0"/>
    <field name="precon_cg" labelOnTop="0"/>
    <field name="preissmann_slot" labelOnTop="0"/>
    <field name="pump_implicit_ratio" labelOnTop="0"/>
    <field name="thin_water_layer_definition" labelOnTop="0"/>
    <field name="use_of_cg" labelOnTop="0"/>
    <field name="use_of_nested_newton" labelOnTop="0"/>
  </labelOnTop>
  <dataDefinedFieldProperties/>
  <widgets/>
  <previewExpression>id</previewExpression>
  <mapTip></mapTip>
  <layerGeometryType>4</layerGeometryType>
</qgis>

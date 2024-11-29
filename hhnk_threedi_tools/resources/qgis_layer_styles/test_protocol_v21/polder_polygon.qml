<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis simplifyLocal="1" readOnly="0" labelsEnabled="1" styleCategories="AllStyleCategories" simplifyAlgorithm="0" hasScaleBasedVisibilityFlag="0" minScale="100000000" simplifyMaxScale="1" version="3.16.11-Hannover" maxScale="0" simplifyDrawingTol="1" simplifyDrawingHints="1">
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
  <renderer-v2 preprocessing="0" forceraster="0" type="invertedPolygonRenderer" enableorderby="0">
    <renderer-v2 symbollevels="0" forceraster="0" type="singleSymbol" enableorderby="0">
      <symbols>
        <symbol type="fill" name="0" alpha="1" clip_to_extent="1" force_rhr="0">
          <layer enabled="1" pass="0" class="SimpleFill" locked="0">
            <prop v="3x:0,0,0,0,0,0" k="border_width_map_unit_scale"/>
            <prop v="0,0,0,255" k="color"/>
            <prop v="bevel" k="joinstyle"/>
            <prop v="0,0" k="offset"/>
            <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
            <prop v="MM" k="offset_unit"/>
            <prop v="0,0,0,255" k="outline_color"/>
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
  </renderer-v2>
  <customproperties>
    <property key="embeddedWidgets/count" value="0"/>
    <property key="variableNames"/>
    <property key="variableValues"/>
  </customproperties>
  <blendMode>0</blendMode>
  <featureBlendMode>0</featureBlendMode>
  <layerOpacity>0.4</layerOpacity>
  <SingleCategoryDiagramRenderer attributeLegend="1" diagramType="Histogram">
    <DiagramCategory penColor="#000000" scaleBasedVisibility="0" spacingUnitScale="3x:0,0,0,0,0,0" backgroundAlpha="255" diagramOrientation="Up" penWidth="0" backgroundColor="#ffffff" direction="1" barWidth="5" opacity="1" lineSizeScale="3x:0,0,0,0,0,0" labelPlacementMethod="XHeight" sizeType="MM" spacingUnit="MM" enabled="0" penAlpha="255" height="15" minimumSize="0" spacing="0" width="15" rotationOffset="270" lineSizeType="MM" showAxis="0" sizeScale="3x:0,0,0,0,0,0" minScaleDenominator="inf" scaleDependency="Area" maxScaleDenominator="1e+08">
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
  <DiagramLayerSettings obstacle="0" zIndex="0" showAll="1" priority="0" dist="0" placement="0" linePlacementFlags="10">
    <properties>
      <Option type="Map">
        <Option type="QString" name="name" value=""/>
        <Option type="Map" name="properties">
          <Option type="Map" name="show">
            <Option type="bool" name="active" value="true"/>
            <Option type="QString" name="field" value="polder_id"/>
            <Option type="int" name="type" value="2"/>
          </Option>
        </Option>
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
    <field name="polder_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="polder_typ" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="cnt_afvrkw" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="name" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="type" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="cnt_pump_e" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="cnt_pump_1" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="cnt_culver" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="cnt_culv_1" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="cnt_weirs_" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="cnt_weirs1" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="cnt_kruisi" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="cnt_krui_1" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="prct_loose" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="prct_noway" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="prct_pump_" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="prct_pump1" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="prct_culve" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="prct_cul_1" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="prct_weirs" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="prct_wei_1" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="prct_kruis" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="prct_kru_1" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="SHAPE_Leng" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="SHAPE_Area" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
  </fieldConfiguration>
  <aliases>
    <alias name="" index="0" field="polder_id"/>
    <alias name="" index="1" field="polder_typ"/>
    <alias name="" index="2" field="cnt_afvrkw"/>
    <alias name="" index="3" field="name"/>
    <alias name="" index="4" field="type"/>
    <alias name="" index="5" field="cnt_pump_e"/>
    <alias name="" index="6" field="cnt_pump_1"/>
    <alias name="" index="7" field="cnt_culver"/>
    <alias name="" index="8" field="cnt_culv_1"/>
    <alias name="" index="9" field="cnt_weirs_"/>
    <alias name="" index="10" field="cnt_weirs1"/>
    <alias name="" index="11" field="cnt_kruisi"/>
    <alias name="" index="12" field="cnt_krui_1"/>
    <alias name="" index="13" field="prct_loose"/>
    <alias name="" index="14" field="prct_noway"/>
    <alias name="" index="15" field="prct_pump_"/>
    <alias name="" index="16" field="prct_pump1"/>
    <alias name="" index="17" field="prct_culve"/>
    <alias name="" index="18" field="prct_cul_1"/>
    <alias name="" index="19" field="prct_weirs"/>
    <alias name="" index="20" field="prct_wei_1"/>
    <alias name="" index="21" field="prct_kruis"/>
    <alias name="" index="22" field="prct_kru_1"/>
    <alias name="" index="23" field="SHAPE_Leng"/>
    <alias name="" index="24" field="SHAPE_Area"/>
  </aliases>
  <defaults>
    <default expression="" applyOnUpdate="0" field="polder_id"/>
    <default expression="" applyOnUpdate="0" field="polder_typ"/>
    <default expression="" applyOnUpdate="0" field="cnt_afvrkw"/>
    <default expression="" applyOnUpdate="0" field="name"/>
    <default expression="" applyOnUpdate="0" field="type"/>
    <default expression="" applyOnUpdate="0" field="cnt_pump_e"/>
    <default expression="" applyOnUpdate="0" field="cnt_pump_1"/>
    <default expression="" applyOnUpdate="0" field="cnt_culver"/>
    <default expression="" applyOnUpdate="0" field="cnt_culv_1"/>
    <default expression="" applyOnUpdate="0" field="cnt_weirs_"/>
    <default expression="" applyOnUpdate="0" field="cnt_weirs1"/>
    <default expression="" applyOnUpdate="0" field="cnt_kruisi"/>
    <default expression="" applyOnUpdate="0" field="cnt_krui_1"/>
    <default expression="" applyOnUpdate="0" field="prct_loose"/>
    <default expression="" applyOnUpdate="0" field="prct_noway"/>
    <default expression="" applyOnUpdate="0" field="prct_pump_"/>
    <default expression="" applyOnUpdate="0" field="prct_pump1"/>
    <default expression="" applyOnUpdate="0" field="prct_culve"/>
    <default expression="" applyOnUpdate="0" field="prct_cul_1"/>
    <default expression="" applyOnUpdate="0" field="prct_weirs"/>
    <default expression="" applyOnUpdate="0" field="prct_wei_1"/>
    <default expression="" applyOnUpdate="0" field="prct_kruis"/>
    <default expression="" applyOnUpdate="0" field="prct_kru_1"/>
    <default expression="" applyOnUpdate="0" field="SHAPE_Leng"/>
    <default expression="" applyOnUpdate="0" field="SHAPE_Area"/>
  </defaults>
  <constraints>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="polder_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="polder_typ"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="cnt_afvrkw"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="name"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="type"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="cnt_pump_e"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="cnt_pump_1"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="cnt_culver"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="cnt_culv_1"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="cnt_weirs_"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="cnt_weirs1"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="cnt_kruisi"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="cnt_krui_1"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="prct_loose"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="prct_noway"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="prct_pump_"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="prct_pump1"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="prct_culve"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="prct_cul_1"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="prct_weirs"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="prct_wei_1"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="prct_kruis"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="prct_kru_1"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="SHAPE_Leng"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="SHAPE_Area"/>
  </constraints>
  <constraintExpressions>
    <constraint desc="" exp="" field="polder_id"/>
    <constraint desc="" exp="" field="polder_typ"/>
    <constraint desc="" exp="" field="cnt_afvrkw"/>
    <constraint desc="" exp="" field="name"/>
    <constraint desc="" exp="" field="type"/>
    <constraint desc="" exp="" field="cnt_pump_e"/>
    <constraint desc="" exp="" field="cnt_pump_1"/>
    <constraint desc="" exp="" field="cnt_culver"/>
    <constraint desc="" exp="" field="cnt_culv_1"/>
    <constraint desc="" exp="" field="cnt_weirs_"/>
    <constraint desc="" exp="" field="cnt_weirs1"/>
    <constraint desc="" exp="" field="cnt_kruisi"/>
    <constraint desc="" exp="" field="cnt_krui_1"/>
    <constraint desc="" exp="" field="prct_loose"/>
    <constraint desc="" exp="" field="prct_noway"/>
    <constraint desc="" exp="" field="prct_pump_"/>
    <constraint desc="" exp="" field="prct_pump1"/>
    <constraint desc="" exp="" field="prct_culve"/>
    <constraint desc="" exp="" field="prct_cul_1"/>
    <constraint desc="" exp="" field="prct_weirs"/>
    <constraint desc="" exp="" field="prct_wei_1"/>
    <constraint desc="" exp="" field="prct_kruis"/>
    <constraint desc="" exp="" field="prct_kru_1"/>
    <constraint desc="" exp="" field="SHAPE_Leng"/>
    <constraint desc="" exp="" field="SHAPE_Area"/>
  </constraintExpressions>
  <expressionfields/>
  <attributeactions/>
  <attributetableconfig actionWidgetStyle="dropDown" sortExpression="" sortOrder="0">
    <columns>
      <column width="-1" type="field" name="polder_id" hidden="0"/>
      <column width="-1" type="field" name="polder_typ" hidden="0"/>
      <column width="-1" type="field" name="cnt_afvrkw" hidden="0"/>
      <column width="-1" type="field" name="name" hidden="0"/>
      <column width="-1" type="field" name="type" hidden="0"/>
      <column width="-1" type="field" name="cnt_pump_e" hidden="0"/>
      <column width="-1" type="field" name="cnt_pump_1" hidden="0"/>
      <column width="-1" type="field" name="cnt_culver" hidden="0"/>
      <column width="-1" type="field" name="cnt_culv_1" hidden="0"/>
      <column width="-1" type="field" name="cnt_weirs_" hidden="0"/>
      <column width="-1" type="field" name="cnt_weirs1" hidden="0"/>
      <column width="-1" type="field" name="cnt_kruisi" hidden="0"/>
      <column width="-1" type="field" name="cnt_krui_1" hidden="0"/>
      <column width="-1" type="field" name="prct_loose" hidden="0"/>
      <column width="-1" type="field" name="prct_noway" hidden="0"/>
      <column width="-1" type="field" name="prct_pump_" hidden="0"/>
      <column width="-1" type="field" name="prct_pump1" hidden="0"/>
      <column width="-1" type="field" name="prct_culve" hidden="0"/>
      <column width="-1" type="field" name="prct_cul_1" hidden="0"/>
      <column width="-1" type="field" name="prct_weirs" hidden="0"/>
      <column width="-1" type="field" name="prct_wei_1" hidden="0"/>
      <column width="-1" type="field" name="prct_kruis" hidden="0"/>
      <column width="-1" type="field" name="prct_kru_1" hidden="0"/>
      <column width="-1" type="field" name="SHAPE_Leng" hidden="0"/>
      <column width="-1" type="field" name="SHAPE_Area" hidden="0"/>
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
from qgis.PyQt.QtWidgets import QWidget

def my_form_open(dialog, layer, feature):
	geom = feature.geometry()
	control = dialog.findChild(QWidget, "MyLineEdit")
]]></editforminitcode>
  <featformsuppress>0</featformsuppress>
  <editorlayout>generatedlayout</editorlayout>
  <editable/>
  <labelOnTop/>
  <dataDefinedFieldProperties/>
  <widgets/>
  <previewExpression>COALESCE( "name", '&lt;NULL>' )</previewExpression>
  <mapTip></mapTip>
  <layerGeometryType>2</layerGeometryType>
</qgis>

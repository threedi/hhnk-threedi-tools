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
  <renderer-v2 symbollevels="0" forceraster="0" type="RuleRenderer" enableorderby="0">
    <rules key="{46fb0dd9-6199-4ba3-a1f9-17b1a60a18d9}">
      <rule symbol="0" key="{d2b14463-2bf9-41ba-8427-0b78aaeb71cc}" label="DuikerSifonHevel_lijn_primair" filter="&quot;WS_CATEGORIE&quot; = 1"/>
      <rule symbol="1" scalemaxdenom="25000" key="{9ae14662-4fc2-418c-b837-b2740d81a30c}" scalemindenom="1" label="DuikerSifonHevel_lijn_secundair" filter="&quot;WS_CATEGORIE&quot; != 1"/>
    </rules>
    <symbols>
      <symbol type="line" name="0" alpha="1" clip_to_extent="1" force_rhr="0">
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
          <prop v="125,125,125,255" k="line_color"/>
          <prop v="solid" k="line_style"/>
          <prop v="1.4" k="line_width"/>
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
      <symbol type="line" name="1" alpha="1" clip_to_extent="1" force_rhr="0">
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
          <prop v="125,125,125,255" k="line_color"/>
          <prop v="solid" k="line_style"/>
          <prop v="1" k="line_width"/>
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
    </symbols>
  </renderer-v2>
  <customproperties>
    <property key="embeddedWidgets/count" value="0"/>
    <property key="variableNames"/>
    <property key="variableValues"/>
  </customproperties>
  <blendMode>0</blendMode>
  <featureBlendMode>0</featureBlendMode>
  <layerOpacity>1</layerOpacity>
  <SingleCategoryDiagramRenderer attributeLegend="1" diagramType="Pie">
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
  <DiagramLayerSettings obstacle="0" zIndex="0" showAll="1" priority="0" dist="0" placement="2" linePlacementFlags="10">
    <properties>
      <Option type="Map">
        <Option type="QString" name="name" value=""/>
        <Option type="Map" name="properties">
          <Option type="Map" name="show">
            <Option type="bool" name="active" value="true"/>
            <Option type="QString" name="field" value="OBJECTID"/>
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
    <field name="OBJECTID" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="CODE" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="NAAM" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="OPMERKING" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="INDICATIEWATERKEREND" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="INDPEILREGULPEILSCHEIDEND" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="LENGTE" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="HOOGTEOPENING" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="BREEDTEOPENING" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="HOOGTEBINNENONDERKANTBENE" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="HOOGTEBINNENONDERKANTBOV" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="VORMKOKER" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="SOORTMATERIAAL" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="TYPEKRUISING" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="WS_CATEGORIE" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="WS_BRON" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="WS_INWINNINGSWIJZE" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="WS_INWINNINGSDATUM" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="WS_INLAATFUNCTIE" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="WS_AFSLUITWIJZE1" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="WS_AFSLUITWIJZE2" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="SHAPE_Length" configurationFlags="None">
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
    <alias name="" index="0" field="OBJECTID"/>
    <alias name="" index="1" field="CODE"/>
    <alias name="" index="2" field="NAAM"/>
    <alias name="" index="3" field="OPMERKING"/>
    <alias name="" index="4" field="INDICATIEWATERKEREND"/>
    <alias name="" index="5" field="INDPEILREGULPEILSCHEIDEND"/>
    <alias name="" index="6" field="LENGTE"/>
    <alias name="" index="7" field="HOOGTEOPENING"/>
    <alias name="" index="8" field="BREEDTEOPENING"/>
    <alias name="" index="9" field="HOOGTEBINNENONDERKANTBENE"/>
    <alias name="" index="10" field="HOOGTEBINNENONDERKANTBOV"/>
    <alias name="" index="11" field="VORMKOKER"/>
    <alias name="" index="12" field="SOORTMATERIAAL"/>
    <alias name="" index="13" field="TYPEKRUISING"/>
    <alias name="" index="14" field="WS_CATEGORIE"/>
    <alias name="" index="15" field="WS_BRON"/>
    <alias name="" index="16" field="WS_INWINNINGSWIJZE"/>
    <alias name="" index="17" field="WS_INWINNINGSDATUM"/>
    <alias name="" index="18" field="WS_INLAATFUNCTIE"/>
    <alias name="" index="19" field="WS_AFSLUITWIJZE1"/>
    <alias name="" index="20" field="WS_AFSLUITWIJZE2"/>
    <alias name="" index="21" field="SHAPE_Length"/>
  </aliases>
  <defaults>
    <default expression="" applyOnUpdate="0" field="OBJECTID"/>
    <default expression="" applyOnUpdate="0" field="CODE"/>
    <default expression="" applyOnUpdate="0" field="NAAM"/>
    <default expression="" applyOnUpdate="0" field="OPMERKING"/>
    <default expression="" applyOnUpdate="0" field="INDICATIEWATERKEREND"/>
    <default expression="" applyOnUpdate="0" field="INDPEILREGULPEILSCHEIDEND"/>
    <default expression="" applyOnUpdate="0" field="LENGTE"/>
    <default expression="" applyOnUpdate="0" field="HOOGTEOPENING"/>
    <default expression="" applyOnUpdate="0" field="BREEDTEOPENING"/>
    <default expression="" applyOnUpdate="0" field="HOOGTEBINNENONDERKANTBENE"/>
    <default expression="" applyOnUpdate="0" field="HOOGTEBINNENONDERKANTBOV"/>
    <default expression="" applyOnUpdate="0" field="VORMKOKER"/>
    <default expression="" applyOnUpdate="0" field="SOORTMATERIAAL"/>
    <default expression="" applyOnUpdate="0" field="TYPEKRUISING"/>
    <default expression="" applyOnUpdate="0" field="WS_CATEGORIE"/>
    <default expression="" applyOnUpdate="0" field="WS_BRON"/>
    <default expression="" applyOnUpdate="0" field="WS_INWINNINGSWIJZE"/>
    <default expression="" applyOnUpdate="0" field="WS_INWINNINGSDATUM"/>
    <default expression="" applyOnUpdate="0" field="WS_INLAATFUNCTIE"/>
    <default expression="" applyOnUpdate="0" field="WS_AFSLUITWIJZE1"/>
    <default expression="" applyOnUpdate="0" field="WS_AFSLUITWIJZE2"/>
    <default expression="" applyOnUpdate="0" field="SHAPE_Length"/>
  </defaults>
  <constraints>
    <constraint unique_strength="1" exp_strength="0" notnull_strength="1" constraints="3" field="OBJECTID"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="CODE"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="NAAM"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="OPMERKING"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="INDICATIEWATERKEREND"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="INDPEILREGULPEILSCHEIDEND"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="LENGTE"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="HOOGTEOPENING"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="BREEDTEOPENING"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="HOOGTEBINNENONDERKANTBENE"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="HOOGTEBINNENONDERKANTBOV"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="VORMKOKER"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="SOORTMATERIAAL"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="TYPEKRUISING"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="WS_CATEGORIE"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="WS_BRON"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="WS_INWINNINGSWIJZE"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="WS_INWINNINGSDATUM"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="WS_INLAATFUNCTIE"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="WS_AFSLUITWIJZE1"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="WS_AFSLUITWIJZE2"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="SHAPE_Length"/>
  </constraints>
  <constraintExpressions>
    <constraint desc="" exp="" field="OBJECTID"/>
    <constraint desc="" exp="" field="CODE"/>
    <constraint desc="" exp="" field="NAAM"/>
    <constraint desc="" exp="" field="OPMERKING"/>
    <constraint desc="" exp="" field="INDICATIEWATERKEREND"/>
    <constraint desc="" exp="" field="INDPEILREGULPEILSCHEIDEND"/>
    <constraint desc="" exp="" field="LENGTE"/>
    <constraint desc="" exp="" field="HOOGTEOPENING"/>
    <constraint desc="" exp="" field="BREEDTEOPENING"/>
    <constraint desc="" exp="" field="HOOGTEBINNENONDERKANTBENE"/>
    <constraint desc="" exp="" field="HOOGTEBINNENONDERKANTBOV"/>
    <constraint desc="" exp="" field="VORMKOKER"/>
    <constraint desc="" exp="" field="SOORTMATERIAAL"/>
    <constraint desc="" exp="" field="TYPEKRUISING"/>
    <constraint desc="" exp="" field="WS_CATEGORIE"/>
    <constraint desc="" exp="" field="WS_BRON"/>
    <constraint desc="" exp="" field="WS_INWINNINGSWIJZE"/>
    <constraint desc="" exp="" field="WS_INWINNINGSDATUM"/>
    <constraint desc="" exp="" field="WS_INLAATFUNCTIE"/>
    <constraint desc="" exp="" field="WS_AFSLUITWIJZE1"/>
    <constraint desc="" exp="" field="WS_AFSLUITWIJZE2"/>
    <constraint desc="" exp="" field="SHAPE_Length"/>
  </constraintExpressions>
  <expressionfields/>
  <attributeactions/>
  <attributetableconfig actionWidgetStyle="dropDown" sortExpression="" sortOrder="0">
    <columns>
      <column width="-1" type="field" name="OBJECTID" hidden="0"/>
      <column width="-1" type="field" name="CODE" hidden="0"/>
      <column width="-1" type="field" name="NAAM" hidden="0"/>
      <column width="-1" type="field" name="OPMERKING" hidden="0"/>
      <column width="-1" type="field" name="INDICATIEWATERKEREND" hidden="0"/>
      <column width="-1" type="field" name="INDPEILREGULPEILSCHEIDEND" hidden="0"/>
      <column width="-1" type="field" name="LENGTE" hidden="0"/>
      <column width="-1" type="field" name="HOOGTEOPENING" hidden="0"/>
      <column width="-1" type="field" name="BREEDTEOPENING" hidden="0"/>
      <column width="-1" type="field" name="HOOGTEBINNENONDERKANTBENE" hidden="0"/>
      <column width="-1" type="field" name="HOOGTEBINNENONDERKANTBOV" hidden="0"/>
      <column width="-1" type="field" name="VORMKOKER" hidden="0"/>
      <column width="-1" type="field" name="SOORTMATERIAAL" hidden="0"/>
      <column width="-1" type="field" name="TYPEKRUISING" hidden="0"/>
      <column width="-1" type="field" name="WS_CATEGORIE" hidden="0"/>
      <column width="-1" type="field" name="WS_BRON" hidden="0"/>
      <column width="-1" type="field" name="WS_INWINNINGSWIJZE" hidden="0"/>
      <column width="-1" type="field" name="WS_INWINNINGSDATUM" hidden="0"/>
      <column width="-1" type="field" name="WS_INLAATFUNCTIE" hidden="0"/>
      <column width="-1" type="field" name="WS_AFSLUITWIJZE1" hidden="0"/>
      <column width="-1" type="field" name="WS_AFSLUITWIJZE2" hidden="0"/>
      <column width="-1" type="field" name="SHAPE_Length" hidden="0"/>
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
from PyQt4.QtGui import QWidget

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
  <previewExpression>"OBJECTID"</previewExpression>
  <mapTip></mapTip>
  <layerGeometryType>1</layerGeometryType>
</qgis>

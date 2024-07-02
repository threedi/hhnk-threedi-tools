<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis simplifyLocal="1" readOnly="0" labelsEnabled="1" styleCategories="AllStyleCategories" simplifyAlgorithm="0" hasScaleBasedVisibilityFlag="0" minScale="100000000" simplifyMaxScale="1" version="3.16.11-Hannover" maxScale="0" simplifyDrawingTol="1" simplifyDrawingHints="0">
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
      <symbol type="marker" name="0" alpha="1" clip_to_extent="1" force_rhr="0">
        <layer enabled="1" pass="0" class="SimpleMarker" locked="0">
          <prop v="0" k="angle"/>
          <prop v="252,120,3,255" k="color"/>
          <prop v="1" k="horizontal_anchor_point"/>
          <prop v="bevel" k="joinstyle"/>
          <prop v="star" k="name"/>
          <prop v="0,0" k="offset"/>
          <prop v="3x:0,0,0,0,0,0" k="offset_map_unit_scale"/>
          <prop v="MM" k="offset_unit"/>
          <prop v="0,0,0,255" k="outline_color"/>
          <prop v="solid" k="outline_style"/>
          <prop v="0" k="outline_width"/>
          <prop v="3x:0,0,0,0,0,0" k="outline_width_map_unit_scale"/>
          <prop v="MM" k="outline_width_unit"/>
          <prop v="area" k="scale_method"/>
          <prop v="4" k="size"/>
          <prop v="3x:0,0,0,0,0,0" k="size_map_unit_scale"/>
          <prop v="MM" k="size_unit"/>
          <prop v="1" k="vertical_anchor_point"/>
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
  <layerOpacity>1</layerOpacity>
  <SingleCategoryDiagramRenderer attributeLegend="1" diagramType="Histogram">
    <DiagramCategory penColor="#000000" scaleBasedVisibility="0" spacingUnitScale="3x:0,0,0,0,0,0" backgroundAlpha="255" diagramOrientation="Up" penWidth="0" backgroundColor="#ffffff" direction="1" barWidth="5" opacity="1" lineSizeScale="3x:0,0,0,0,0,0" labelPlacementMethod="XHeight" sizeType="MM" spacingUnit="MM" enabled="0" penAlpha="255" height="15" minimumSize="0" spacing="0" width="15" rotationOffset="270" lineSizeType="MM" showAxis="0" sizeScale="3x:0,0,0,0,0,0" minScaleDenominator="inf" scaleDependency="Area" maxScaleDenominator="1e+08">
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
  <DiagramLayerSettings obstacle="0" zIndex="0" showAll="1" priority="0" dist="0" placement="0" linePlacementFlags="10">
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
    <field name="OBJECTID" configurationFlags="None">
      <editWidget type="">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="organisation_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="created" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="code" configurationFlags="None">
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
    <field name="crest_width" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="crest_level" configurationFlags="None">
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
    <field name="lat_dis_coeff" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="angle" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="allowed_flow_direction" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="controlled" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="comment" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="discharge_coeff" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="image_url" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="num_timeseries" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="weir_shape" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="controlled_fdla_code" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="channel_code" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="type_function" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="opmerking" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="wgtype_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="channel_type_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="opmerk_datamining" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="crest_height_datamining" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="aanname" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="isusable" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="QString" name="IsMultiline" value="0"/>
            <Option type="QString" name="UseHtml" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="hasassumption" configurationFlags="None">
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
    <alias name="" index="1" field="id"/>
    <alias name="" index="2" field="organisation_id"/>
    <alias name="" index="3" field="created"/>
    <alias name="" index="4" field="code"/>
    <alias name="" index="5" field="type"/>
    <alias name="" index="6" field="crest_width"/>
    <alias name="" index="7" field="crest_level"/>
    <alias name="" index="8" field="name"/>
    <alias name="" index="9" field="lat_dis_coeff"/>
    <alias name="" index="10" field="angle"/>
    <alias name="" index="11" field="allowed_flow_direction"/>
    <alias name="" index="12" field="controlled"/>
    <alias name="" index="13" field="comment"/>
    <alias name="" index="14" field="discharge_coeff"/>
    <alias name="" index="15" field="image_url"/>
    <alias name="" index="16" field="num_timeseries"/>
    <alias name="" index="17" field="weir_shape"/>
    <alias name="" index="18" field="controlled_fdla_code"/>
    <alias name="" index="19" field="channel_code"/>
    <alias name="" index="20" field="type_function"/>
    <alias name="" index="21" field="opmerking"/>
    <alias name="" index="22" field="wgtype_id"/>
    <alias name="" index="23" field="channel_type_id"/>
    <alias name="" index="24" field="opmerk_datamining"/>
    <alias name="" index="25" field="crest_height_datamining"/>
    <alias name="" index="26" field="aanname"/>
    <alias name="" index="27" field="isusable"/>
    <alias name="" index="28" field="hasassumption"/>
  </aliases>
  <defaults>
    <default expression="" applyOnUpdate="0" field="OBJECTID"/>
    <default expression="" applyOnUpdate="0" field="id"/>
    <default expression="" applyOnUpdate="0" field="organisation_id"/>
    <default expression="" applyOnUpdate="0" field="created"/>
    <default expression="" applyOnUpdate="0" field="code"/>
    <default expression="" applyOnUpdate="0" field="type"/>
    <default expression="" applyOnUpdate="0" field="crest_width"/>
    <default expression="" applyOnUpdate="0" field="crest_level"/>
    <default expression="" applyOnUpdate="0" field="name"/>
    <default expression="" applyOnUpdate="0" field="lat_dis_coeff"/>
    <default expression="" applyOnUpdate="0" field="angle"/>
    <default expression="" applyOnUpdate="0" field="allowed_flow_direction"/>
    <default expression="" applyOnUpdate="0" field="controlled"/>
    <default expression="" applyOnUpdate="0" field="comment"/>
    <default expression="" applyOnUpdate="0" field="discharge_coeff"/>
    <default expression="" applyOnUpdate="0" field="image_url"/>
    <default expression="" applyOnUpdate="0" field="num_timeseries"/>
    <default expression="" applyOnUpdate="0" field="weir_shape"/>
    <default expression="" applyOnUpdate="0" field="controlled_fdla_code"/>
    <default expression="" applyOnUpdate="0" field="channel_code"/>
    <default expression="" applyOnUpdate="0" field="type_function"/>
    <default expression="" applyOnUpdate="0" field="opmerking"/>
    <default expression="" applyOnUpdate="0" field="wgtype_id"/>
    <default expression="" applyOnUpdate="0" field="channel_type_id"/>
    <default expression="" applyOnUpdate="0" field="opmerk_datamining"/>
    <default expression="" applyOnUpdate="0" field="crest_height_datamining"/>
    <default expression="" applyOnUpdate="0" field="aanname"/>
    <default expression="" applyOnUpdate="0" field="isusable"/>
    <default expression="" applyOnUpdate="0" field="hasassumption"/>
  </defaults>
  <constraints>
    <constraint unique_strength="1" exp_strength="0" notnull_strength="1" constraints="3" field="OBJECTID"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="organisation_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="created"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="code"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="type"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="crest_width"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="crest_level"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="name"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="lat_dis_coeff"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="angle"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="allowed_flow_direction"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="controlled"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="comment"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="discharge_coeff"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="image_url"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="num_timeseries"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="weir_shape"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="controlled_fdla_code"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="channel_code"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="type_function"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="opmerking"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="wgtype_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="channel_type_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="opmerk_datamining"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="crest_height_datamining"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="aanname"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="isusable"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="hasassumption"/>
  </constraints>
  <constraintExpressions>
    <constraint desc="" exp="" field="OBJECTID"/>
    <constraint desc="" exp="" field="id"/>
    <constraint desc="" exp="" field="organisation_id"/>
    <constraint desc="" exp="" field="created"/>
    <constraint desc="" exp="" field="code"/>
    <constraint desc="" exp="" field="type"/>
    <constraint desc="" exp="" field="crest_width"/>
    <constraint desc="" exp="" field="crest_level"/>
    <constraint desc="" exp="" field="name"/>
    <constraint desc="" exp="" field="lat_dis_coeff"/>
    <constraint desc="" exp="" field="angle"/>
    <constraint desc="" exp="" field="allowed_flow_direction"/>
    <constraint desc="" exp="" field="controlled"/>
    <constraint desc="" exp="" field="comment"/>
    <constraint desc="" exp="" field="discharge_coeff"/>
    <constraint desc="" exp="" field="image_url"/>
    <constraint desc="" exp="" field="num_timeseries"/>
    <constraint desc="" exp="" field="weir_shape"/>
    <constraint desc="" exp="" field="controlled_fdla_code"/>
    <constraint desc="" exp="" field="channel_code"/>
    <constraint desc="" exp="" field="type_function"/>
    <constraint desc="" exp="" field="opmerking"/>
    <constraint desc="" exp="" field="wgtype_id"/>
    <constraint desc="" exp="" field="channel_type_id"/>
    <constraint desc="" exp="" field="opmerk_datamining"/>
    <constraint desc="" exp="" field="crest_height_datamining"/>
    <constraint desc="" exp="" field="aanname"/>
    <constraint desc="" exp="" field="isusable"/>
    <constraint desc="" exp="" field="hasassumption"/>
  </constraintExpressions>
  <expressionfields/>
  <attributeactions/>
  <attributetableconfig actionWidgetStyle="dropDown" sortExpression="" sortOrder="0">
    <columns>
      <column width="-1" type="field" name="id" hidden="0"/>
      <column width="-1" type="field" name="organisation_id" hidden="0"/>
      <column width="-1" type="field" name="created" hidden="0"/>
      <column width="-1" type="field" name="code" hidden="0"/>
      <column width="-1" type="field" name="type" hidden="0"/>
      <column width="-1" type="field" name="crest_width" hidden="0"/>
      <column width="-1" type="field" name="crest_level" hidden="0"/>
      <column width="-1" type="field" name="name" hidden="0"/>
      <column width="-1" type="field" name="lat_dis_coeff" hidden="0"/>
      <column width="-1" type="field" name="angle" hidden="0"/>
      <column width="-1" type="field" name="allowed_flow_direction" hidden="0"/>
      <column width="-1" type="field" name="controlled" hidden="0"/>
      <column width="-1" type="field" name="comment" hidden="0"/>
      <column width="-1" type="field" name="discharge_coeff" hidden="0"/>
      <column width="-1" type="field" name="image_url" hidden="0"/>
      <column width="-1" type="field" name="num_timeseries" hidden="0"/>
      <column width="-1" type="field" name="weir_shape" hidden="0"/>
      <column width="-1" type="field" name="controlled_fdla_code" hidden="0"/>
      <column width="-1" type="field" name="channel_code" hidden="0"/>
      <column width="-1" type="field" name="type_function" hidden="0"/>
      <column width="-1" type="field" name="opmerking" hidden="0"/>
      <column width="-1" type="field" name="wgtype_id" hidden="0"/>
      <column width="-1" type="field" name="channel_type_id" hidden="0"/>
      <column width="-1" type="field" name="opmerk_datamining" hidden="0"/>
      <column width="-1" type="field" name="crest_height_datamining" hidden="0"/>
      <column width="-1" type="field" name="aanname" hidden="0"/>
      <column width="-1" type="field" name="isusable" hidden="0"/>
      <column width="-1" type="field" name="hasassumption" hidden="0"/>
      <column width="-1" type="field" name="orig_ogc_fid" hidden="0"/>
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
  <layerGeometryType>0</layerGeometryType>
</qgis>

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
    <field name="maximum_sim_time_step" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="nr_timesteps" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="dem_file" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="minimum_sim_time_step" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
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
    <field name="table_step_size_volume_2d" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="frict_coef_file" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="initial_groundwater_level_file" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="initial_waterlevel" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="epsg_code" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="numerical_settings_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="dem_obstacle_detection" configurationFlags="None">
      <editWidget type="CheckBox">
        <config>
          <Option type="Map">
            <Option type="QString" name="CheckedState" value="1"/>
            <Option type="QString" name="UncheckedState" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="frict_avg" configurationFlags="None">
      <editWidget type="CheckBox">
        <config>
          <Option type="Map">
            <Option type="QString" name="CheckedState" value="1"/>
            <Option type="QString" name="UncheckedState" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="initial_groundwater_level_type" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="max" value="0"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="min" value="1"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="average" value="2"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="water_level_ini_type" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="max" value="0"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="min" value="1"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="average" value="2"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="grid_space" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="advection_2d" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="0: Do not use advection 2d" value="0"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="1: Use advection 2d" value="1"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="embedded_cutoff_threshold" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="dist_calc_points" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="start_date" configurationFlags="None">
      <editWidget type="DateTime">
        <config>
          <Option type="Map">
            <Option type="bool" name="allow_null" value="true"/>
            <Option type="bool" name="calendar_popup" value="true"/>
            <Option type="QString" name="display_format" value="yyyy-MM-dd"/>
            <Option type="QString" name="field_format" value="yyyy-MM-dd"/>
            <Option type="bool" name="field_iso_format" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="max_interception_file" configurationFlags="None">
      <editWidget type="">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="output_time_step" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="interflow_settings_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="table_step_size" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="use_1d_flow" configurationFlags="None">
      <editWidget type="CheckBox">
        <config>
          <Option type="Map">
            <Option type="QString" name="CheckedState" value="1"/>
            <Option type="QString" name="UncheckedState" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="start_time" configurationFlags="None">
      <editWidget type="DateTime">
        <config>
          <Option type="Map">
            <Option type="bool" name="allow_null" value="true"/>
            <Option type="bool" name="calendar_popup" value="true"/>
            <Option type="QString" name="display_format" value="yyyy-MM-dd HH:mm:ss"/>
            <Option type="QString" name="field_format" value="yyyy-MM-dd HH:mm:ss"/>
            <Option type="bool" name="field_iso_format" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="use_2d_rain" configurationFlags="None">
      <editWidget type="CheckBox">
        <config>
          <Option type="Map">
            <Option type="QString" name="CheckedState" value="1"/>
            <Option type="QString" name="UncheckedState" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="initial_groundwater_level" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="kmax" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="initial_waterlevel_file" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="sim_time_step" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="frict_coef" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="guess_dams" configurationFlags="None">
      <editWidget type="CheckBox">
        <config>
          <Option type="Map">
            <Option type="QString" name="CheckedState" value="1"/>
            <Option type="QString" name="UncheckedState" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="control_group_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="dem_obstacle_height" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="timestep_plus" configurationFlags="None">
      <editWidget type="CheckBox">
        <config>
          <Option type="Map">
            <Option type="QString" name="CheckedState" value="1"/>
            <Option type="QString" name="UncheckedState" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="name" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="flooding_threshold" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="frict_type" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="1: Chèzy" value="1"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="2: Manning" value="2"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="use_2d_flow" configurationFlags="None">
      <editWidget type="CheckBox">
        <config>
          <Option type="Map">
            <Option type="QString" name="CheckedState" value="1"/>
            <Option type="QString" name="UncheckedState" value="0"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="max_interception" configurationFlags="None">
      <editWidget type="">
        <config>
          <Option/>
        </config>
      </editWidget>
    </field>
    <field name="max_angle_1d_advection" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="advection_1d" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="0: Do not use advection 1d" value="0"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="1: Use advection 1d" value="1"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="2: Experimental advection 1d" value="2"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="3: Experimental advection 1d" value="3"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="4: Experimental advection 1d" value="4"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="5: Experimental advection 1d" value="5"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="6: Experimental advection 1d" value="6"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="wind_shielding_file" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="simple_infiltration_settings_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="groundwater_settings_id" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="manhole_storage_area" configurationFlags="None">
      <editWidget type="TextEdit">
        <config>
          <Option type="Map">
            <Option type="bool" name="IsMultiline" value="false"/>
            <Option type="bool" name="UseHtml" value="false"/>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="use_0d_inflow" configurationFlags="None">
      <editWidget type="ValueMap">
        <config>
          <Option type="Map">
            <Option type="List" name="map">
              <Option type="Map">
                <Option type="QString" name="0: do not use 0d inflow" value="0"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="1: use v2_impervious_surface" value="1"/>
              </Option>
              <Option type="Map">
                <Option type="QString" name="2: use v2_surface" value="2"/>
              </Option>
            </Option>
          </Option>
        </config>
      </editWidget>
    </field>
    <field name="table_step_size_1d" configurationFlags="None">
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
    <alias name="" index="0" field="maximum_sim_time_step"/>
    <alias name="" index="1" field="nr_timesteps"/>
    <alias name="" index="2" field="dem_file"/>
    <alias name="" index="3" field="minimum_sim_time_step"/>
    <alias name="" index="4" field="id"/>
    <alias name="" index="5" field="table_step_size_volume_2d"/>
    <alias name="" index="6" field="frict_coef_file"/>
    <alias name="" index="7" field="initial_groundwater_level_file"/>
    <alias name="" index="8" field="initial_waterlevel"/>
    <alias name="" index="9" field="epsg_code"/>
    <alias name="" index="10" field="numerical_settings_id"/>
    <alias name="" index="11" field="dem_obstacle_detection"/>
    <alias name="" index="12" field="frict_avg"/>
    <alias name="" index="13" field="initial_groundwater_level_type"/>
    <alias name="" index="14" field="water_level_ini_type"/>
    <alias name="" index="15" field="grid_space"/>
    <alias name="" index="16" field="advection_2d"/>
    <alias name="" index="17" field="embedded_cutoff_threshold"/>
    <alias name="" index="18" field="dist_calc_points"/>
    <alias name="" index="19" field="start_date"/>
    <alias name="" index="20" field="max_interception_file"/>
    <alias name="" index="21" field="output_time_step"/>
    <alias name="" index="22" field="interflow_settings_id"/>
    <alias name="" index="23" field="table_step_size"/>
    <alias name="" index="24" field="use_1d_flow"/>
    <alias name="" index="25" field="start_time"/>
    <alias name="" index="26" field="use_2d_rain"/>
    <alias name="" index="27" field="initial_groundwater_level"/>
    <alias name="" index="28" field="kmax"/>
    <alias name="" index="29" field="initial_waterlevel_file"/>
    <alias name="" index="30" field="sim_time_step"/>
    <alias name="" index="31" field="frict_coef"/>
    <alias name="" index="32" field="guess_dams"/>
    <alias name="" index="33" field="control_group_id"/>
    <alias name="" index="34" field="dem_obstacle_height"/>
    <alias name="" index="35" field="timestep_plus"/>
    <alias name="" index="36" field="name"/>
    <alias name="" index="37" field="flooding_threshold"/>
    <alias name="" index="38" field="frict_type"/>
    <alias name="" index="39" field="use_2d_flow"/>
    <alias name="" index="40" field="max_interception"/>
    <alias name="" index="41" field="max_angle_1d_advection"/>
    <alias name="" index="42" field="advection_1d"/>
    <alias name="" index="43" field="wind_shielding_file"/>
    <alias name="" index="44" field="simple_infiltration_settings_id"/>
    <alias name="" index="45" field="groundwater_settings_id"/>
    <alias name="" index="46" field="manhole_storage_area"/>
    <alias name="" index="47" field="use_0d_inflow"/>
    <alias name="" index="48" field="table_step_size_1d"/>
  </aliases>
  <defaults>
    <default expression="" applyOnUpdate="0" field="maximum_sim_time_step"/>
    <default expression="" applyOnUpdate="0" field="nr_timesteps"/>
    <default expression="" applyOnUpdate="0" field="dem_file"/>
    <default expression="" applyOnUpdate="0" field="minimum_sim_time_step"/>
    <default expression="if(maximum(id) is null,1, maximum(id)+1)" applyOnUpdate="0" field="id"/>
    <default expression="" applyOnUpdate="0" field="table_step_size_volume_2d"/>
    <default expression="" applyOnUpdate="0" field="frict_coef_file"/>
    <default expression="" applyOnUpdate="0" field="initial_groundwater_level_file"/>
    <default expression="" applyOnUpdate="0" field="initial_waterlevel"/>
    <default expression="" applyOnUpdate="0" field="epsg_code"/>
    <default expression="1" applyOnUpdate="0" field="numerical_settings_id"/>
    <default expression="0" applyOnUpdate="0" field="dem_obstacle_detection"/>
    <default expression="0" applyOnUpdate="0" field="frict_avg"/>
    <default expression="" applyOnUpdate="0" field="initial_groundwater_level_type"/>
    <default expression="" applyOnUpdate="0" field="water_level_ini_type"/>
    <default expression="" applyOnUpdate="0" field="grid_space"/>
    <default expression="" applyOnUpdate="0" field="advection_2d"/>
    <default expression="" applyOnUpdate="0" field="embedded_cutoff_threshold"/>
    <default expression="10000" applyOnUpdate="0" field="dist_calc_points"/>
    <default expression=" to_date(now() )" applyOnUpdate="0" field="start_date"/>
    <default expression="" applyOnUpdate="0" field="max_interception_file"/>
    <default expression="" applyOnUpdate="0" field="output_time_step"/>
    <default expression="" applyOnUpdate="0" field="interflow_settings_id"/>
    <default expression="0.01" applyOnUpdate="0" field="table_step_size"/>
    <default expression="" applyOnUpdate="0" field="use_1d_flow"/>
    <default expression=" to_date( now() ) ||  ' 00:00:00'" applyOnUpdate="0" field="start_time"/>
    <default expression="" applyOnUpdate="0" field="use_2d_rain"/>
    <default expression="" applyOnUpdate="0" field="initial_groundwater_level"/>
    <default expression="" applyOnUpdate="0" field="kmax"/>
    <default expression="" applyOnUpdate="0" field="initial_waterlevel_file"/>
    <default expression="" applyOnUpdate="0" field="sim_time_step"/>
    <default expression="" applyOnUpdate="0" field="frict_coef"/>
    <default expression="0" applyOnUpdate="0" field="guess_dams"/>
    <default expression="" applyOnUpdate="0" field="control_group_id"/>
    <default expression="" applyOnUpdate="0" field="dem_obstacle_height"/>
    <default expression="0" applyOnUpdate="0" field="timestep_plus"/>
    <default expression="" applyOnUpdate="0" field="name"/>
    <default expression="0.001" applyOnUpdate="0" field="flooding_threshold"/>
    <default expression="2" applyOnUpdate="0" field="frict_type"/>
    <default expression="" applyOnUpdate="0" field="use_2d_flow"/>
    <default expression="" applyOnUpdate="0" field="max_interception"/>
    <default expression="" applyOnUpdate="0" field="max_angle_1d_advection"/>
    <default expression="" applyOnUpdate="0" field="advection_1d"/>
    <default expression="" applyOnUpdate="0" field="wind_shielding_file"/>
    <default expression="" applyOnUpdate="0" field="simple_infiltration_settings_id"/>
    <default expression="" applyOnUpdate="0" field="groundwater_settings_id"/>
    <default expression="" applyOnUpdate="0" field="manhole_storage_area"/>
    <default expression="" applyOnUpdate="0" field="use_0d_inflow"/>
    <default expression="" applyOnUpdate="0" field="table_step_size_1d"/>
  </defaults>
  <constraints>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="maximum_sim_time_step"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="nr_timesteps"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="dem_file"/>
    <constraint unique_strength="0" exp_strength="2" notnull_strength="0" constraints="4" field="minimum_sim_time_step"/>
    <constraint unique_strength="1" exp_strength="0" notnull_strength="1" constraints="3" field="id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="table_step_size_volume_2d"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="frict_coef_file"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="initial_groundwater_level_file"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="initial_waterlevel"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="epsg_code"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="numerical_settings_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="dem_obstacle_detection"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="frict_avg"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="initial_groundwater_level_type"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="water_level_ini_type"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="grid_space"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="advection_2d"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="embedded_cutoff_threshold"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="dist_calc_points"/>
    <constraint unique_strength="0" exp_strength="2" notnull_strength="2" constraints="5" field="start_date"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="max_interception_file"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="output_time_step"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="interflow_settings_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="table_step_size"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="use_1d_flow"/>
    <constraint unique_strength="0" exp_strength="2" notnull_strength="2" constraints="5" field="start_time"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="use_2d_rain"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="initial_groundwater_level"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="kmax"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="initial_waterlevel_file"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="sim_time_step"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="frict_coef"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="guess_dams"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="control_group_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="dem_obstacle_height"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="timestep_plus"/>
    <constraint unique_strength="1" exp_strength="0" notnull_strength="2" constraints="3" field="name"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="flooding_threshold"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="frict_type"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="use_2d_flow"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="max_interception"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="max_angle_1d_advection"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="advection_1d"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="wind_shielding_file"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="simple_infiltration_settings_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="groundwater_settings_id"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="manhole_storage_area"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="2" constraints="1" field="use_0d_inflow"/>
    <constraint unique_strength="0" exp_strength="0" notnull_strength="0" constraints="0" field="table_step_size_1d"/>
  </constraints>
  <constraintExpressions>
    <constraint desc="" exp="" field="maximum_sim_time_step"/>
    <constraint desc="" exp="" field="nr_timesteps"/>
    <constraint desc="" exp="" field="dem_file"/>
    <constraint desc="" exp=" &quot;minimum_sim_time_step&quot; &lt; &quot;sim_time_step&quot; " field="minimum_sim_time_step"/>
    <constraint desc="" exp="" field="id"/>
    <constraint desc="" exp="" field="table_step_size_volume_2d"/>
    <constraint desc="" exp="" field="frict_coef_file"/>
    <constraint desc="" exp="" field="initial_groundwater_level_file"/>
    <constraint desc="" exp="" field="initial_waterlevel"/>
    <constraint desc="" exp="" field="epsg_code"/>
    <constraint desc="" exp="" field="numerical_settings_id"/>
    <constraint desc="" exp="" field="dem_obstacle_detection"/>
    <constraint desc="" exp="" field="frict_avg"/>
    <constraint desc="" exp="" field="initial_groundwater_level_type"/>
    <constraint desc="" exp="" field="water_level_ini_type"/>
    <constraint desc="" exp="" field="grid_space"/>
    <constraint desc="" exp="" field="advection_2d"/>
    <constraint desc="" exp="" field="embedded_cutoff_threshold"/>
    <constraint desc="" exp="" field="dist_calc_points"/>
    <constraint desc="" exp="&quot;start_date&quot; is not null" field="start_date"/>
    <constraint desc="" exp="" field="max_interception_file"/>
    <constraint desc="" exp="" field="output_time_step"/>
    <constraint desc="" exp="" field="interflow_settings_id"/>
    <constraint desc="" exp="" field="table_step_size"/>
    <constraint desc="" exp="" field="use_1d_flow"/>
    <constraint desc="" exp="&quot;start_time&quot;" field="start_time"/>
    <constraint desc="" exp="" field="use_2d_rain"/>
    <constraint desc="" exp="" field="initial_groundwater_level"/>
    <constraint desc="" exp="" field="kmax"/>
    <constraint desc="" exp="" field="initial_waterlevel_file"/>
    <constraint desc="" exp="" field="sim_time_step"/>
    <constraint desc="" exp="" field="frict_coef"/>
    <constraint desc="" exp="" field="guess_dams"/>
    <constraint desc="" exp="" field="control_group_id"/>
    <constraint desc="" exp="" field="dem_obstacle_height"/>
    <constraint desc="" exp="" field="timestep_plus"/>
    <constraint desc="" exp="" field="name"/>
    <constraint desc="" exp="" field="flooding_threshold"/>
    <constraint desc="" exp="" field="frict_type"/>
    <constraint desc="" exp="" field="use_2d_flow"/>
    <constraint desc="" exp="" field="max_interception"/>
    <constraint desc="" exp="" field="max_angle_1d_advection"/>
    <constraint desc="" exp="" field="advection_1d"/>
    <constraint desc="" exp="" field="wind_shielding_file"/>
    <constraint desc="" exp="" field="simple_infiltration_settings_id"/>
    <constraint desc="" exp="" field="groundwater_settings_id"/>
    <constraint desc="" exp="" field="manhole_storage_area"/>
    <constraint desc="" exp="" field="use_0d_inflow"/>
    <constraint desc="" exp="" field="table_step_size_1d"/>
  </constraintExpressions>
  <expressionfields/>
  <attributeactions>
    <defaultAction key="Canvas" value="{00000000-0000-0000-0000-000000000000}"/>
  </attributeactions>
  <attributetableconfig actionWidgetStyle="dropDown" sortExpression="&quot;maximum_sim_time_step&quot;" sortOrder="0">
    <columns>
      <column width="116" type="field" name="maximum_sim_time_step" hidden="0"/>
      <column width="-1" type="field" name="nr_timesteps" hidden="0"/>
      <column width="-1" type="field" name="dem_file" hidden="0"/>
      <column width="-1" type="field" name="minimum_sim_time_step" hidden="0"/>
      <column width="-1" type="field" name="id" hidden="0"/>
      <column width="-1" type="field" name="table_step_size_volume_2d" hidden="0"/>
      <column width="-1" type="field" name="frict_coef_file" hidden="0"/>
      <column width="-1" type="field" name="initial_groundwater_level_file" hidden="0"/>
      <column width="-1" type="field" name="initial_waterlevel" hidden="0"/>
      <column width="-1" type="field" name="epsg_code" hidden="0"/>
      <column width="-1" type="field" name="numerical_settings_id" hidden="0"/>
      <column width="-1" type="field" name="dem_obstacle_detection" hidden="0"/>
      <column width="-1" type="field" name="frict_avg" hidden="0"/>
      <column width="-1" type="field" name="initial_groundwater_level_type" hidden="0"/>
      <column width="-1" type="field" name="water_level_ini_type" hidden="0"/>
      <column width="-1" type="field" name="grid_space" hidden="0"/>
      <column width="-1" type="field" name="advection_2d" hidden="0"/>
      <column width="-1" type="field" name="embedded_cutoff_threshold" hidden="0"/>
      <column width="-1" type="field" name="dist_calc_points" hidden="0"/>
      <column width="-1" type="field" name="start_date" hidden="0"/>
      <column width="-1" type="field" name="initial_groundwater_level" hidden="0"/>
      <column width="-1" type="field" name="output_time_step" hidden="0"/>
      <column width="-1" type="field" name="interflow_settings_id" hidden="0"/>
      <column width="-1" type="field" name="table_step_size" hidden="0"/>
      <column width="-1" type="field" name="use_1d_flow" hidden="0"/>
      <column width="-1" type="field" name="start_time" hidden="0"/>
      <column width="-1" type="field" name="use_2d_rain" hidden="0"/>
      <column width="-1" type="field" name="kmax" hidden="0"/>
      <column width="-1" type="field" name="initial_waterlevel_file" hidden="0"/>
      <column width="-1" type="field" name="sim_time_step" hidden="0"/>
      <column width="-1" type="field" name="frict_coef" hidden="0"/>
      <column width="-1" type="field" name="guess_dams" hidden="0"/>
      <column width="-1" type="field" name="control_group_id" hidden="0"/>
      <column width="-1" type="field" name="dem_obstacle_height" hidden="0"/>
      <column width="-1" type="field" name="timestep_plus" hidden="0"/>
      <column width="-1" type="field" name="name" hidden="0"/>
      <column width="-1" type="field" name="flooding_threshold" hidden="0"/>
      <column width="-1" type="field" name="frict_type" hidden="0"/>
      <column width="-1" type="field" name="use_2d_flow" hidden="0"/>
      <column width="-1" type="field" name="max_angle_1d_advection" hidden="0"/>
      <column width="-1" type="field" name="advection_1d" hidden="0"/>
      <column width="-1" type="field" name="wind_shielding_file" hidden="0"/>
      <column width="-1" type="field" name="simple_infiltration_settings_id" hidden="0"/>
      <column width="-1" type="field" name="groundwater_settings_id" hidden="0"/>
      <column width="-1" type="field" name="manhole_storage_area" hidden="0"/>
      <column width="-1" type="field" name="use_0d_inflow" hidden="0"/>
      <column width="-1" type="field" name="table_step_size_1d" hidden="0"/>
      <column width="-1" type="actions" hidden="1"/>
      <column width="-1" type="field" name="interception_global" hidden="0"/>
      <column width="-1" type="field" name="interception_file" hidden="0"/>
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
      <attributeEditorField name="id" showLabel="1" index="4"/>
      <attributeEditorField name="name" showLabel="1" index="36"/>
      <attributeEditorField name="use_0d_inflow" showLabel="1" index="47"/>
      <attributeEditorField name="use_1d_flow" showLabel="1" index="24"/>
      <attributeEditorField name="use_2d_rain" showLabel="1" index="26"/>
      <attributeEditorField name="use_2d_flow" showLabel="1" index="39"/>
    </attributeEditorContainer>
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Grid" visibilityExpression="" groupBox="0" showLabel="1">
      <attributeEditorField name="grid_space" showLabel="1" index="15"/>
      <attributeEditorField name="kmax" showLabel="1" index="28"/>
      <attributeEditorField name="table_step_size" showLabel="1" index="23"/>
    </attributeEditorContainer>
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Terrain information" visibilityExpression="&quot;advection_1d&quot;" groupBox="0" showLabel="1">
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="DEM" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="dem_file" showLabel="1" index="2"/>
        <attributeEditorField name="epsg_code" showLabel="1" index="9"/>
      </attributeEditorContainer>
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Friction" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="frict_coef_file" showLabel="1" index="6"/>
        <attributeEditorField name="frict_coef" showLabel="1" index="31"/>
        <attributeEditorField name="frict_type" showLabel="1" index="38"/>
        <attributeEditorField name="frict_avg" showLabel="1" index="12"/>
      </attributeEditorContainer>
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Groundwater" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="initial_groundwater_level_file" showLabel="1" index="7"/>
        <attributeEditorField name="initial_groundwater_level" showLabel="1" index="27"/>
        <attributeEditorField name="initial_groundwater_level_type" showLabel="1" index="13"/>
      </attributeEditorContainer>
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Initial waterlevel" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="initial_waterlevel_file" showLabel="1" index="29"/>
        <attributeEditorField name="initial_waterlevel" showLabel="1" index="8"/>
        <attributeEditorField name="water_level_ini_type" showLabel="1" index="14"/>
      </attributeEditorContainer>
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Interception" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="interception_file" showLabel="1" index="-1"/>
        <attributeEditorField name="interception_global" showLabel="1" index="-1"/>
        <attributeEditorField name="max_interception" showLabel="1" index="40"/>
      </attributeEditorContainer>
      <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Wind" visibilityExpression="" groupBox="1" showLabel="1">
        <attributeEditorField name="wind_shielding_file" showLabel="1" index="43"/>
      </attributeEditorContainer>
    </attributeEditorContainer>
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Time" visibilityExpression="" groupBox="0" showLabel="1">
      <attributeEditorField name="start_date" showLabel="1" index="19"/>
      <attributeEditorField name="start_time" showLabel="1" index="25"/>
      <attributeEditorField name="sim_time_step" showLabel="1" index="30"/>
      <attributeEditorField name="timestep_plus" showLabel="1" index="35"/>
      <attributeEditorField name="minimum_sim_time_step" showLabel="1" index="3"/>
      <attributeEditorField name="maximum_sim_time_step" showLabel="1" index="0"/>
      <attributeEditorField name="nr_timesteps" showLabel="1" index="1"/>
      <attributeEditorField name="output_time_step" showLabel="1" index="21"/>
    </attributeEditorContainer>
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Settings id's" visibilityExpression="" groupBox="0" showLabel="1">
      <attributeEditorField name="interflow_settings_id" showLabel="1" index="22"/>
      <attributeEditorField name="groundwater_settings_id" showLabel="1" index="45"/>
      <attributeEditorField name="numerical_settings_id" showLabel="1" index="10"/>
      <attributeEditorField name="simple_infiltration_settings_id" showLabel="1" index="44"/>
      <attributeEditorField name="control_group_id" showLabel="1" index="33"/>
    </attributeEditorContainer>
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Extra options 1D" visibilityExpression="" groupBox="0" showLabel="1">
      <attributeEditorField name="advection_1d" showLabel="1" index="42"/>
      <attributeEditorField name="dist_calc_points" showLabel="1" index="18"/>
      <attributeEditorField name="manhole_storage_area" showLabel="1" index="46"/>
      <attributeEditorField name="max_angle_1d_advection" showLabel="1" index="41"/>
      <attributeEditorField name="table_step_size_1d" showLabel="1" index="48"/>
    </attributeEditorContainer>
    <attributeEditorContainer columnCount="1" visibilityExpressionEnabled="0" name="Extra options 2D" visibilityExpression="" groupBox="0" showLabel="1">
      <attributeEditorField name="advection_2d" showLabel="1" index="16"/>
      <attributeEditorField name="dem_obstacle_detection" showLabel="1" index="11"/>
      <attributeEditorField name="guess_dams" showLabel="1" index="32"/>
      <attributeEditorField name="dem_obstacle_height" showLabel="1" index="34"/>
      <attributeEditorField name="embedded_cutoff_threshold" showLabel="1" index="17"/>
      <attributeEditorField name="flooding_threshold" showLabel="1" index="37"/>
      <attributeEditorField name="table_step_size_volume_2d" showLabel="1" index="5"/>
    </attributeEditorContainer>
  </attributeEditorForm>
  <editable>
    <field name="advection_1d" editable="1"/>
    <field name="advection_2d" editable="1"/>
    <field name="control_group_id" editable="1"/>
    <field name="dem_file" editable="1"/>
    <field name="dem_obstacle_detection" editable="1"/>
    <field name="dem_obstacle_height" editable="1"/>
    <field name="dist_calc_points" editable="1"/>
    <field name="embedded_cutoff_threshold" editable="1"/>
    <field name="epsg_code" editable="1"/>
    <field name="flooding_threshold" editable="1"/>
    <field name="frict_avg" editable="1"/>
    <field name="frict_coef" editable="1"/>
    <field name="frict_coef_file" editable="1"/>
    <field name="frict_type" editable="1"/>
    <field name="grid_space" editable="1"/>
    <field name="groundwater_settings_id" editable="1"/>
    <field name="guess_dams" editable="1"/>
    <field name="id" editable="1"/>
    <field name="initial_groundwater_level" editable="1"/>
    <field name="initial_groundwater_level_file" editable="1"/>
    <field name="initial_groundwater_level_type" editable="1"/>
    <field name="initial_waterlevel" editable="1"/>
    <field name="initial_waterlevel_file" editable="1"/>
    <field name="interception_file" editable="1"/>
    <field name="interception_global" editable="1"/>
    <field name="interflow_settings_id" editable="1"/>
    <field name="kmax" editable="1"/>
    <field name="manhole_storage_area" editable="1"/>
    <field name="max_angle_1d_advection" editable="1"/>
    <field name="max_interception" editable="1"/>
    <field name="max_interception_file" editable="1"/>
    <field name="maximum_sim_time_step" editable="1"/>
    <field name="minimum_sim_time_step" editable="1"/>
    <field name="name" editable="1"/>
    <field name="nr_timesteps" editable="1"/>
    <field name="numerical_settings_id" editable="1"/>
    <field name="output_time_step" editable="1"/>
    <field name="sim_time_step" editable="1"/>
    <field name="simple_infiltration_settings_id" editable="1"/>
    <field name="start_date" editable="1"/>
    <field name="start_time" editable="1"/>
    <field name="table_step_size" editable="1"/>
    <field name="table_step_size_1d" editable="1"/>
    <field name="table_step_size_volume_2d" editable="1"/>
    <field name="timestep_plus" editable="1"/>
    <field name="use_0d_inflow" editable="1"/>
    <field name="use_1d_flow" editable="1"/>
    <field name="use_2d_flow" editable="1"/>
    <field name="use_2d_rain" editable="1"/>
    <field name="water_level_ini_type" editable="1"/>
    <field name="wind_shielding_file" editable="1"/>
  </editable>
  <labelOnTop>
    <field name="advection_1d" labelOnTop="0"/>
    <field name="advection_2d" labelOnTop="0"/>
    <field name="control_group_id" labelOnTop="0"/>
    <field name="dem_file" labelOnTop="0"/>
    <field name="dem_obstacle_detection" labelOnTop="0"/>
    <field name="dem_obstacle_height" labelOnTop="0"/>
    <field name="dist_calc_points" labelOnTop="0"/>
    <field name="embedded_cutoff_threshold" labelOnTop="0"/>
    <field name="epsg_code" labelOnTop="0"/>
    <field name="flooding_threshold" labelOnTop="0"/>
    <field name="frict_avg" labelOnTop="0"/>
    <field name="frict_coef" labelOnTop="0"/>
    <field name="frict_coef_file" labelOnTop="0"/>
    <field name="frict_type" labelOnTop="0"/>
    <field name="grid_space" labelOnTop="0"/>
    <field name="groundwater_settings_id" labelOnTop="0"/>
    <field name="guess_dams" labelOnTop="0"/>
    <field name="id" labelOnTop="0"/>
    <field name="initial_groundwater_level" labelOnTop="0"/>
    <field name="initial_groundwater_level_file" labelOnTop="0"/>
    <field name="initial_groundwater_level_type" labelOnTop="0"/>
    <field name="initial_waterlevel" labelOnTop="0"/>
    <field name="initial_waterlevel_file" labelOnTop="0"/>
    <field name="interception_file" labelOnTop="0"/>
    <field name="interception_global" labelOnTop="0"/>
    <field name="interflow_settings_id" labelOnTop="0"/>
    <field name="kmax" labelOnTop="0"/>
    <field name="manhole_storage_area" labelOnTop="0"/>
    <field name="max_angle_1d_advection" labelOnTop="0"/>
    <field name="max_interception" labelOnTop="0"/>
    <field name="max_interception_file" labelOnTop="0"/>
    <field name="maximum_sim_time_step" labelOnTop="0"/>
    <field name="minimum_sim_time_step" labelOnTop="0"/>
    <field name="name" labelOnTop="0"/>
    <field name="nr_timesteps" labelOnTop="0"/>
    <field name="numerical_settings_id" labelOnTop="0"/>
    <field name="output_time_step" labelOnTop="0"/>
    <field name="sim_time_step" labelOnTop="0"/>
    <field name="simple_infiltration_settings_id" labelOnTop="0"/>
    <field name="start_date" labelOnTop="0"/>
    <field name="start_time" labelOnTop="0"/>
    <field name="table_step_size" labelOnTop="0"/>
    <field name="table_step_size_1d" labelOnTop="0"/>
    <field name="table_step_size_volume_2d" labelOnTop="0"/>
    <field name="timestep_plus" labelOnTop="0"/>
    <field name="use_0d_inflow" labelOnTop="0"/>
    <field name="use_1d_flow" labelOnTop="0"/>
    <field name="use_2d_flow" labelOnTop="0"/>
    <field name="use_2d_rain" labelOnTop="0"/>
    <field name="water_level_ini_type" labelOnTop="0"/>
    <field name="wind_shielding_file" labelOnTop="0"/>
  </labelOnTop>
  <dataDefinedFieldProperties/>
  <widgets/>
  <previewExpression>id</previewExpression>
  <mapTip></mapTip>
  <layerGeometryType>4</layerGeometryType>
</qgis>

# Schematisation Scenario Builder

Original code; hhnk_threedi_tools\core\schematisation\model_splitter.py

To make scenarios calculations we need to make multiple schematisations on Rana with
different settings. Changes are made in one central schematisation (00_basis), and once we want to start
a calculation we create the required scenario and upload them to Rana.

This module provides a structured system for building these schematisations:

- `ScenarioSettings`: loads and validates configuration.
- `ScenarioBuilder`: constructs complete schematisations.
- `ScenarioModifiers`: add scenario‑specific transformations.
- `RanaSchematisationService`: handles upload and revision data.
- `ScenarioService`: ties everything together for batch workflows.


## Overwegingen
- default settings worden nu overschreven maar is eigenlijk niet handig. Beter zou zijn om de schematisation checker een check op te nemen die een warning geeft als je van de defaults afwijkt.


Column changes:

See migration guide; https://docs.ranawaterintelligence.com/h_schema_300.html

- max_infiltration_capacity_file -> max_infiltration_volume_file
- simple_infiltration_settings_id -> use_simple_infiltration [bool]
- "v2_simple_infiltration.display_name": "glg" -> removed.

- "model_settings.nr_grid_levels": 3, -> staat overal op 3, dus kan default worden?
- water_level_ini_type -> initial_water_level_aggregation -> alles null, dus weg?


dtype mismatches; https://docs.ranawaterintelligence.com/d_settings_objects.html 

simulation_template_settings.name -- dtype=int -- typo?
simulation_template_settings.use_0d_inflow --dtype=bool -- In model lijkt het een int. -> overzetten naar bool?

time_step_settings.min_time_step -- dtype=int? we hebben na migratie hier een float staan met 0.01. Draaien van schematisation check geeft geen warning. Is dit de bedoeling dat het een int wordt of float?

model_settings.use_2d_rain -- dtype=bool? in https://docs.ranawaterintelligence.com/_downloads/84ae42a0608fb89dbd6d9ba1eefce34f/3Di%20database%20schema%20219%20to%20schema%20300.xlsx staat deze op int. In de database lijkt het ook nog int.

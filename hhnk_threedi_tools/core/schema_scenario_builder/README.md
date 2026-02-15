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
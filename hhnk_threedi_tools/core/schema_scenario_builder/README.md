# Schematisation Scenario Builder

Original code; hhnk_threedi_tools\core\schematisation\model_splitter.py

To make scenarios calculations we need to make multiple schematisations on Rana with
different settings. In the normal workflow, changes are made in one central schematisation (00_basis), and once we want to start
a calculation we create the required scenario and upload them to Rana. These scenarios should NOT be manually edited after should be completely managed by the ScenarioService.

This module provides a structured system for building these schematisations:

- `models`: `BaseSchematisationLayer` and subclasses define the different setting layers of the schematisation.
- `ScenarioBuilder`: constructs complete schematisations, copies from base and applies changes from json.
- `RanaSchematisationService`: handles upload and revision data.
- `ScenarioService`: ties everything together for batch workflows.

For this builder to work the file `02_schematisation\schematisation_scenarios.json` is required. It holds the settings for each scenario. This file is generated during schematisation building. For older schematisations, use `migrate_scenario_xlsx_to_json.py` to converts the `02_schematisation/model_settings.xlsx` to the json format.

Example json:

```json
{
    "0d1d_check": {
        "schematisation_name": "model_test_v2__0d1d_test",
        "layers": {
            "simulation_template_settings": {
                "name": "0d1d_check",
                "use_0d_inflow": true,
                "use_structure_control": false
            },
            "initial_conditions": {
                "initial_water_level_file": null,
                "initial_water_level_aggregation": null
            },
            "time_step_settings": {
                "output_time_step": 900
            },
            "model_settings": {
                "nr_grid_levels": 3,
                "dem_file": "dem_hoekje.tif",
                "friction_coefficient_file": null,
                "use_2d_rain": false,
                "use_2d_flow": false,
                "use_simple_infiltration": false
            }
        }
    },
}
```

## Overwegingen

- default settings worden in oude modelsplliter overschreven maar is eigenlijk niet handig. Beter zou zijn om de schematisation checker een check op te nemen die een warning geeft als je van de defaults afwijkt.


Column changes v219 -> v300;

See migration guide; https://docs.ranawaterintelligence.com/h_schema_300.html

- max_infiltration_capacity_file -> max_infiltration_volume_file
- simple_infiltration_settings_id -> use_simple_infiltration [bool]
- "v2_simple_infiltration.display_name": "glg" -> removed.

- "model_settings.nr_grid_levels": 3, -> staat overal op 3, dus kan default worden?


Questions for NenS:
dtype mismatches; https://docs.ranawaterintelligence.com/d_settings_objects.html 

simulation_template_settings.name -- dtype=int -- typo?
simulation_template_settings.use_0d_inflow --dtype=bool -- In model lijkt het een int. -> overzetten naar bool?

time_step_settings.min_time_step -- dtype=int? we hebben na migratie hier een float staan met 0.01. Draaien van schematisation check geeft geen warning. Is dit de bedoeling dat het een int wordt of float?

model_settings.use_2d_rain -- dtype=bool? in https://docs.ranawaterintelligence.com/_downloads/84ae42a0608fb89dbd6d9ba1eefce34f/3Di%20database%20schema%20219%20to%20schema%20300.xlsx staat deze op int. In de database lijkt het ook nog int.

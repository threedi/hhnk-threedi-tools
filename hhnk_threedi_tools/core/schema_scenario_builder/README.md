# Schematisation Scenario Builder

Original code; hhnk_threedi_tools\core\schematisation\model_splitter.py

To make scenarios calculations we need to make multiple schematisations on Threedi.

Class: ScenarioBuilder
The main entry point — replaces ModelSchematisations.
Handles:

copying sqlite + rasters
applying settings to sqlite tables
scenario‑specific modifications
extra SQL from JSON
uploading
local revision creation

Methods
Public


build(name)
Main function. Creates scenario folder, copies files, edits sqlite, applies modifier.


upload(name, commit_message, api_key, organisation_uuid)
Upload new scenario to 3Di.


create_local_revision(commit_message)
Save sqlite + settings as “revision”.


get_latest_local_revision_str()
Human‑readable summary of last revision.


get_model_revision_info(name, api_key)
Query API for last uploaded model revision.



Internal helpers (stay inside builder.py)


_copy_sqlite(schema_base, schema_new)


_copy_rasters(row, schema_base, schema_new)


_write_global_settings(db, row, defaults)


_write_simple_infiltration(db, row, defaults)


_apply_modifier(db, name)
Delegates to modifiers.py.


_apply_extra_sql(db, scenario_name)


These helpers keep build() readable.

📁 2. settings.py
Class: ScenarioSettings
Loads your Excel files and gives easy access to scenario rows.
Methods

__init__(settings_path, defaults_path)
get(name) → returns the row for a scenario
list() → list of scenario names
_load_settings()
_load_defaults()
_sanity_check()
Optional: warn on duplicate columns
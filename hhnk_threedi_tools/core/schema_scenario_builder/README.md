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
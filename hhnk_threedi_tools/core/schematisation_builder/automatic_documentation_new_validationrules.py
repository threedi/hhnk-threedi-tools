import json

import hhnk_research_tools as hrt
import pandas as pd

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources

validation_rules_json_path = hrt.get_pkg_resource_path(schematisation_builder_resources, "validationrules.json")


with open(validation_rules_json_path) as val:
    val_rules = json.load(val)


# create empty dataframe
table_rules = pd.DataFrame(columns=["laag", "type functie", "naam", "beschrijving"])

for object_layer in val_rules["objects"]:
    for general_function in object_layer["general_rules"]:
        if general_function["id"] >= 100:
            new_func = {
                "laag": object_layer["object"],
                "type_function": "general_rules",
                "naam": general_function["name"],
                "beschrijving": general_function["description"],
            }
            # TODO: voeg new rule toe aan dataframe, weet code effe niet
    for val_rule in object_layer["validation_rules"]:
        if val_rule["validation_rule_set"] == "hhnk":
            new_rule = {
                "laag": object_layer["object"],
                "type_function": "validation_rules",
                "naam": val_rule["name"],
                "beschrijving": val_rule["description"],
            }
            # TODO: voeg toe

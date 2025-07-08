import json

import hhnk_research_tools as hrt
import pandas as pd

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources

validation_rules_json_path = hrt.get_pkg_resource_path(schematisation_builder_resources, "validationrules.json")


with open(validation_rules_json_path) as val:
    val_rules = json.load(val)


# create empty dataframe
table_rules = pd.DataFrame(columns=["laag", "type_functie", "naam", "beschrijving"])

for object_layer in val_rules["objects"]:
    if object_layer.get("general_rules"):
        for general_function in object_layer["general_rules"]:
            # print(general_function)
            if general_function["id"] >= 100:
                new_func = {
                    "laag": object_layer["object"],
                    "type_functie": "general rules",
                    "naam": general_function["result_variable"],
                    "beschrijving": general_function["description"],
                }
                # TODO: voeg new rule toe aan dataframe, weet code effe niet
                table_rules = pd.concat([table_rules, pd.DataFrame([new_func])], ignore_index=True)

    for val_rule in object_layer["validation_rules"]:
        if val_rule["validation_rule_set"] == "hhnk":
            new_rule = {
                "laag": object_layer["object"],
                "type_functie": "validation rules",
                "naam": val_rule["name"],
                "beschrijving": val_rule["description"],
            }
            # TODO: voeg toe
            table_rules = pd.concat([table_rules, pd.DataFrame([new_rule])], ignore_index=True)

# save to csv
table_rules.to_csv(hrt.get_pkg_resource_path(schematisation_builder_resources, "hhnk_validationrules.csv"))

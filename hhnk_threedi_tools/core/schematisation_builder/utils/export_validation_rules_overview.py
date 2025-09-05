# %%
import json

import hhnk_research_tools as hrt
import pandas as pd

from hhnk_threedi_tools.resources import schematisation_builder as sb_resources


# %%
def export_new_validation_rules_and_functions_overview():
    json_path = hrt.get_pkg_resource_path(sb_resources, "validationrules.json")
    with open(json_path) as f:
        rules = json.load(f)

    rows = []

    for obj in rules["objects"]:
        if obj.get("general_rules"):
            for func in obj["general_rules"]:
                if func["id"] >= 100:
                    rows.append(
                        {
                            "id": int(func["id"]),
                            "laag": obj["object"],
                            "type_functie": "general rules",
                            "naam": func["result_variable"],
                            "beschrijving": func["description"],
                        }
                    )

        for rule in obj["validation_rules"]:
            if rule["validation_rule_set"] == "hhnk":
                rows.append(
                    {
                        "id": int(rule["id"]),
                        "laag": obj["object"],
                        "type_functie": "validation rules",
                        "naam": rule["name"],
                        "beschrijving": rule["description"],
                    }
                )

    df = pd.DataFrame(rows, columns=["id", "laag", "type_functie", "naam", "beschrijving"])
    output_path = hrt.get_pkg_resource_path(sb_resources, "hhnk_validationrules.csv")
    df.to_csv(output_path, index=False)


def export_validation_rules_overview(rule_set: str = "basic"):
    json_path = hrt.get_pkg_resource_path(sb_resources, "validationrules.json")
    with open(json_path) as f:
        rules = json.load(f)

    rows = []

    for obj in rules["objects"]:
        for rule in obj["validation_rules"]:
            if rule["validation_rule_set"] == rule_set:
                rows.append(
                    {
                        "id": int(rule["id"]),
                        "laag": obj["object"],
                        "type_functie": "validation rules",
                        "naam": rule["name"],
                    }
                )

    df = pd.DataFrame(rows, columns=["id", "laag", "type_functie", "naam"])
    output_path = hrt.get_pkg_resource_path(sb_resources, f"{rule_set}_validationrules.csv")
    df.to_csv(output_path, index=False)


# %%
if __name__ == "__main__":
    export_new_validation_rules_and_functions_overview()
    export_validation_rules_overview("basic")
# %%

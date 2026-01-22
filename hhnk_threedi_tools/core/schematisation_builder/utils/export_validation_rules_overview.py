# %%
import json

import hhnk_research_tools as hrt
import pandas as pd

from hhnk_threedi_tools.resources import schematisation_builder as sb_resources


# %%
def export_validation_rules_overview():
    json_path = hrt.get_pkg_resource_path(sb_resources, "validationrules.json")
    with open(json_path) as f:
        rules = json.load(f)

    rows = []

    for obj in rules["objects"]:
        if obj.get("general_rules"):
            for func in obj["general_rules"]:
                rule_set = "hwh"
                # check if func["description"] exists
                try:
                    func["description"]
                except KeyError:
                    func["description"] = ""

                if func["id"] >= 100:
                    rule_set = "hhnk"
                rows.append(
                    {
                        "id": int(func["id"]),
                        "bron": rule_set,
                        "laag": obj["object"],
                        "type_functie": "general rules",
                        "naam": func["result_variable"],
                        "beschrijving": func["description"],
                    }
                )

        for rule in obj["validation_rules"]:
            rule_set = "hwh"
            # check if func["description"] exists
            try:
                rule["description"]
            except KeyError:
                rule["description"] = ""
            if rule["validation_rule_set"] == "hhnk" or rule["id"] >= 100:
                rule_set = "hhnk"
            if rule["active"]:
                rows.append(
                    {
                        "id": int(rule["id"]),
                        "bron": rule_set,
                        "laag": obj["object"],
                        "type_functie": "validation rules",
                        "naam": rule["name"],
                        "beschrijving": rule["description"],
                    }
                )
    # construct and sort dataframe
    df = pd.DataFrame(rows, columns=["bron", "laag", "id", "type_functie", "naam", "beschrijving"])
    df = df.sort_values(by=["type_functie", "bron"]).reset_index(drop=True)

    val_df = df[df["type_functie"] == "validation rules"].reset_index(drop=True)
    fun_df = df[df["type_functie"] == "general rules"].reset_index(drop=True)

    val_fp = hrt.get_pkg_resource_path(sb_resources, "validation_rules.csv")
    fun_fp = hrt.get_pkg_resource_path(sb_resources, "general_rules.csv")

    val_df.to_csv(val_fp, index=True)
    fun_df.to_csv(fun_fp, index=True)


# %%
if __name__ == "__main__":
    export_validation_rules_overview()

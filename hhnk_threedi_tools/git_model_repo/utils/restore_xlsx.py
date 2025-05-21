import json
import os

import pandas as pd


class ExcelRestore(object):
    def __init__(self, excel_path, output_file_path=None):
        self.excel_path = excel_path

        if output_file_path is None:
            filename = os.path.basename(excel_path.rtrim("_xlsx")) + "_restored.xlsx"
            output_file_path = os.path.join(os.path.dirname(excel_path), os.pardir, filename)
        self.output_file_path = output_file_path

    def restore(self):
        """Restore the sheets and features of the excel file from a json-file."""

        # get all json files in the directory
        schema = json.load(open(os.path.join(self.excel_path, "schema.json"), "r"))
        with pd.ExcelWriter(self.output_file_path, engine="openpyxl", mode="w") as writer:
            for sheet_name, sheet_schema in schema.items():
                json_file_path = os.path.join(self.excel_path, f"{sheet_name}.json")
                df = pd.read_json(json_file_path, orient="records", lines=True, dtype=sheet_schema)
                df.to_excel(writer, index=False, sheet_name=sheet_name)

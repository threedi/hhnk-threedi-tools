import json
import os

import pandas as pd


class ExcelDump(object):


    def __init__(self, file_path, output_path=None):
        self.file_path = file_path

        if output_path is None:
            base = os.path.splitext(os.path.basename(file_path))
            output_path = os.path.join(
                os.path.dirname(file_path),
                f"{base[0]}_{base[1]}"
            )
        self.output_path = output_path
        os.makedirs(self.output_path, exist_ok=True)

    def get_schema(self):
        """get schema of the excel datamodel as dictionary.
        """

        xls = pd.ExcelFile(self.file_path, engine='openpyxl')
        schema = {}
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(self.file_path, sheet_name=sheet_name, engine='openpyxl')
            schema[sheet_name] = df.dtypes.astype(str).to_dict()
            # df.dtypes.astype(str).to_dict()
        return schema

    def dump_schema(self):
        """Dump the schema of the excel datamodel to a json file.
        """

        schema = self.get_schema()

        with open(os.path.join(self.output_path, 'schema.json'), 'w') as fp:
            json.dump(schema, fp, indent=2)

    def dump_sheets(self):
        """Dump the sheets and features of the excel file to a json-file.
        """

        xls = pd.ExcelFile(self.file_path, engine='openpyxl')
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(self.file_path, sheet_name=sheet_name, engine='openpyxl')
            output_path = os.path.join(self.output_path, f"{sheet_name}.json")
            df.to_json(output_path, orient='records', lines=True)

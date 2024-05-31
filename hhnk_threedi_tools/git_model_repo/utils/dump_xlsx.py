import json
import os

import pandas as pd

from .file_change_detection import FileChangeDetection


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
        self.changed_files = []

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
        file_path = os.path.join(self.output_path, 'schema.json')
        cd = FileChangeDetection(file_path)

        schema = self.get_schema()
        with open(file_path, 'w') as fp:
            json.dump(schema, fp, indent=2)

        if cd.has_changed():
            self.changed_files.append(file_path)

    def dump_sheets(self):
        """Dump the sheets and features of the excel file to a json-file.
        """

        xls = pd.ExcelFile(self.file_path, engine='openpyxl')
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(self.file_path, sheet_name=sheet_name, engine='openpyxl')
            output_file_path = os.path.join(self.output_path, f"{sheet_name}.json")
            cd = FileChangeDetection(output_file_path)
            df.to_json(output_file_path, orient='records', lines=True)
            if cd.has_changed():
                self.changed_files.append(output_file_path)

import json
from pathlib import Path

import pandas as pd


class ExcelRestore(object):
    """Restore an Excel file from schema and JSON files.

    Parameters
    ----------
    excel_path : Path
        Path to the folder containing the JSON files and the schema.json file.
    output_file_path : Path, optional
        Path to the output Excel file. If None, the output file will be stored
        in the parent directory of the input directory.

    Attributes
    ----------
    excel_path : Path
        Path to the input folder.
    output_file_path : Path
        Path to the output Excel file.

    Methods
    -------
    restore()
        Restore the Excel file from the directory.
    """

    def __init__(self, excel_path: Path, output_file_path: Path = None):
        """Initialize the ExcelRestore object.

        Parameters
        ----------
        excel_path : Path
            Path to the folder containing the JSON files and the schema.json file.
        output_file_path : Path, optional
            Path to the output Excel file. If None, the output file will be stored
            in the parent directory of the input directory.
        """
        self.excel_path = Path(excel_path)

        if output_file_path is None:
            filename = self.excel_path.name.rstrip("_xlsx") + "_restored.xlsx"
            output_file_path = self.excel_path.parent.parent / filename
        self.output_file_path = Path(output_file_path)

    def restore(self):
        """Restore the sheets and features of the Excel file from JSON files.

        Returns
        -------
        None
        """
        schema_file = self.excel_path / "schema.json"
        with schema_file.open("r") as f:
            schema = json.load(f)
        with pd.ExcelWriter(self.output_file_path, engine="openpyxl", mode="w") as writer:
            for sheet_name, sheet_schema in schema.items():
                json_file_path = self.excel_path / f"{sheet_name}.json"
                df = pd.read_json(json_file_path, orient="records", lines=True, dtype=sheet_schema)
                df.to_excel(writer, index=False, sheet_name=sheet_name)

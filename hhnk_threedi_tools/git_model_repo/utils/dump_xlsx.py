import json
import os
from pathlib import Path
from typing import List, Optional

import pandas as pd

from .file_change_detection import FileChangeDetection


class ExcelDump(object):
    """
    Basic version for dumping Excel files to JSON files.

    Parameters
    ----------
    file_path : Path
        Path to the Excel file.
    output_path : Path, optional
        Path to the output directory. If None, a default directory is created next to the input file.

    Attributes
    ----------
    file_path : Path
        Path to the Excel file.
    output_path : Path
        Path to the output directory.
    changed_files : list of Path
        List of files that have changed after dumping.

    Methods
    -------
    get_schema()
        Get the schema of the Excel datamodel as a dictionary.
    dump_schema()
        Dump the schema to a JSON file.
    dump_sheets()
        Dump all sheets and their data to JSON files.
    """
    def __init__(self, file_path: Path, output_path: Optional[Path] = None):
        """Initialize the ExcelDump object.

        Parameters
        ----------
        file_path : Path
            Path to the Excel file.
        output_path : Path, optional
            Path to the output directory. If None, a default directory is created.
        """
        self.file_path = file_path

        if output_path is None:
            base = self.file_path.stem
            ext = self.file_path.suffix
            output_path = self.file_path.parent / f"{base}_{ext}"
        self.output_path = output_path
        self.output_path.mkdir(exist_ok=True)
        self.changed_files: List[Path] = []

    def get_schema(self) -> dict:
        """Get the schema of the Excel datamodel as a dictionary.

        Returns
        -------
        dict
            Dictionary with sheet names as keys and column types as values.
        """
        xls = pd.ExcelFile(self.file_path, engine="openpyxl")
        schema = {}
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(self.file_path, sheet_name=sheet_name, engine="openpyxl")
            schema[sheet_name] = df.dtypes.astype(str).to_dict()
        return schema

    def dump_schema(self):
        """Dump the schema of the Excel datamodel to a JSON file.

        Returns
        -------
        None
        """
        file_path = self.output_path / "schema.json"
        cd = FileChangeDetection(file_path)

        schema = self.get_schema()
        with file_path.open("w") as fp:
            json.dump(schema, fp, indent=2)

        if cd.has_changed():
            self.changed_files.append(file_path)

    def dump_sheets(self):
        """
        Dump the sheets and features of the Excel file to JSON files.

        Returns
        -------
        None
        """
        xls = pd.ExcelFile(self.file_path, engine="openpyxl")
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(self.file_path, sheet_name=sheet_name, engine="openpyxl")
            output_file_path = self.output_path / f"{sheet_name}.json"
            cd = FileChangeDetection(output_file_path)
            df.to_json(output_file_path, orient="records", lines=True)
            if cd.has_changed():
                self.changed_files.append(output_file_path)
"""Folderstructuur voor de hydamo validatie."""
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Union

import hhnk_research_tools as hrt

@dataclass
class SourcePaths:
    """Locaties van inputbestanden voor het converteren van DAMO naar HYDAMO"""

    damo_file: str
    conversion_files_folder: str
    cso_file: str
    hdb_file: str

    def __post_init__(self):
        """
        damo_file(str): path to damo
        conversion_files_folder (str): path to folder with conversion files
        cso_file (str): path to cso
        hdb_file (str): path to hdb
        """

        self.damo = hrt.File(self.damo_file)
        self.conversion_files = hrt.Folder(self.conversion_files_folder)
        self.cso = hrt.File(self.cso_file)
        self.hdb = hrt.File(self.hdb_file)
        self.verify()

    def verify(self):
        """Verify check if inputs exist"""
        filesnotfound = []
        for f in [
            self.damo,
            self.conversion_files,
            self.cso,
            self.hdb
        ]:
            try:
                if not f.exists():
                    filesnotfound.append(f.base)
            except RuntimeError as e:
                raise Exception(f"{f.base}") from e

        if filesnotfound:
            raise FileNotFoundError(f"{filesnotfound} not found")

        return True
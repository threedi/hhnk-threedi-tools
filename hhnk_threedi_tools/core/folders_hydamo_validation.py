"""Folderstructuur voor de hydamo validatie."""
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Union

import hhnk_research_tools as hrt

@dataclass
class SourcePaths:
    """Locaties van inputbestanden voor het valideren van de hydamo"""

    dem_file: Union[str, Path, hrt.Raster]
    hydamo_file: str
    rules_file: str

    def __post_init__(self):
        """
        dem_file(str): raster source van DEM
        hydamo_file (str): path to hydamo
        rules_file (str): path to rules
        """

        self.dem = hrt.Raster(self.dem_file)
        self.hydamo = hrt.File(self.hydamo_file)
        self.rules = hrt.File(self.rules_file)
        self.verify()

    def verify(self):
        """Verify check if inputs exist"""
        filesnotfound = []
        for f in [
            self.dem,
            self.hydamo,
            self.rules
        ]:
            try:
                if not f.exists():
                    filesnotfound.append(f.base)
            except RuntimeError as e:
                raise Exception(f"{f.base}") from e

        if filesnotfound:
            raise FileNotFoundError(f"{filesnotfound} not found")

        return True

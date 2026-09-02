import json
import os
import time
import warnings
import zipfile
from pathlib import Path
from typing import Union

import hhnk_research_tools as hrt
import pandas as pd
import requests
from breaches.rasters import breach_wdepth_damage

from hhnk_threedi_tools.breaches.breaches import Breaches
from hhnk_threedi_tools.breaches.ldo import upload_files_ldo


def get_schade_getroffen(excel_path: Union[str, Path]) -> None:
    """Populate damage and affected counts for scenarios in an Excel file.

    Reads an LDO API key from `api_ldo_key.txt`, opens ``excel_path`` (expects
    a sheet named "Sheet1") and finds scenarios where the column
    "Totaalschade" is null. For each scenario the function queries the LDO
    API for external processing results and writes back the values
    ``Totaal getroffenen``, ``Totaalschade``, ``Totaal slachtoffers`` and
    ``Type`` into the dataframe. The modified dataframe is saved to
    ``excel_path`` in a sheet named "Blad2".

    Parameters
    ----------
    excel_path : Union[str, Path]
        Path or string pointing to the Excel file to read and update.

    Side effects
    -----------
    - Requires a file named ``api_ldo_key.txt`` in the current working
      directory containing a JSON-encoded API key for LDO.
    - Writes the updated dataframe back to ``excel_path`` (sheet "Blad2").

    Notes
    -----
    - The function sleeps 10 seconds between API requests to avoid rate
      limiting.
    - All exceptions during per-scenario processing are caught and logged;
      the function continues with the next scenario.

    Returns
    -------
    None
    """

    raw = Path("api_ldo_key.txt").read_text("utf8")
    LDO_API_KEY = json.loads(raw)
    check_excel = pd.read_excel(excel_path, sheet_name="Sheet1")

    # scenario_id = check_excel["Scenario ID"].values
    scenario_id = check_excel.loc[check_excel["Totaalschade"].isnull(), "Scenario ID"].to_list()
    ldo_api = upload_files_ldo.LDO_API(api_key=LDO_API_KEY)
    sleeptime = 10
    for scenario in scenario_id:
        try:
            # keep original placeholder expression

            data_schade = ldo_api.get_external_processings(scenario)
            items = data_schade.get("items", [])
            if not items:
                print(f"Scenario {scenario} not found")
                continue
            else:
                Totaal_getroffenen = data_schade["items"][0]["meta_data"]["Totaal getroffenen"]
                Totaalschade = data_schade["items"][0]["meta_data"]["Totaalschade"]
                Totaal_slachtoffers = data_schade["items"][0]["meta_data"]["Totaal slachtoffers"]
                processing_type = data_schade["items"][0]["type"]

                check_excel.loc[check_excel["Scenario ID"] == scenario, "Totaal getroffenen"] = Totaal_getroffenen
                check_excel.loc[check_excel["Scenario ID"] == scenario, "Totaalschade"] = Totaalschade
                check_excel.loc[check_excel["Scenario ID"] == scenario, "Totaal slachtoffers"] = Totaal_slachtoffers
                check_excel.loc[check_excel["Scenario ID"] == scenario, "Type"] = processing_type
                print(f"Processed scenario {scenario}")

        except Exception as e:
            logger.error(f"Error processing scenario {scenario}: {e}")
        time.sleep(sleeptime)

    # Save once at the end
    with pd.ExcelWriter(excel_path, engine="openpyxl", mode="w") as writer:
        check_excel.to_excel(writer, index=False, sheet_name="Blad2")


# %%

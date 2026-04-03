# %%
import os
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
from threedi_api_client.api import ThreediApi
from threedi_api_client.openapi import ApiException
from threedi_api_client.versions import V3Api
from threedi_scenario_downloader import downloader as dl
from threedigrid.admin.gridresultadmin import GridH5ResultAdmin


# %%
def _get_api_client() -> V3Api:
    api_keys_path = (
        rf"{os.getenv('APPDATA')}\3Di\QGIS3\profiles\default\python\plugins\hhnk_threedi_plugin\api_key.txt"
    )

    api_key = hrt.read_api_file(api_keys_path)
    config = {
        "THREEDI_API_HOST": "https://api.3di.live",
        "THREEDI_API_PERSONAL_API_TOKEN": api_key["threedi"],
    }
    api_client: V3Api = ThreediApi(config=config, version="v3-beta")

    try:
        user = api_client.auth_profile_list()
    except ApiException as exc:
        raise RuntimeError("Could not log in to 3Di API. Check your API key.") from exc
    else:
        print(f"Successfully logged in as {user.username}!")

    return api_client


def _get_scalar(series: pd.Series, column_name: str, simulation_name: str):
    if series.empty:
        raise ValueError(f"No value found for '{column_name}' in scenario '{simulation_name}'.")
    return series.values[0]


def _get_breach_info(
    simulation_name: str,
    region_path: Path,
    metadata_df_ns: pd.DataFrame,
    bresen_df: gpd.GeoDataFrame,
) -> dict:
    simulations_data = os.listdir(region_path)

    coordinate_x = _get_scalar(
        bresen_df.loc[bresen_df["EQ_naam"] == simulation_name, "x-coordina"],
        "x-coordina",
        simulation_name,
    )
    coordinate_y = _get_scalar(
        bresen_df.loc[bresen_df["EQ_naam"] == simulation_name, "y-coordina"],
        "y-coordina",
        simulation_name,
    )
    naam_waterkering = _get_scalar(
        bresen_df.loc[bresen_df["EQ_naam"] == simulation_name, "Naam water"],
        "Naam water",
        simulation_name,
    )
    initial_crest_level = _get_scalar(
        bresen_df.loc[bresen_df["EQ_naam"] == simulation_name, "In_Cr_lvl"],
        "In_Cr_lvl",
        simulation_name,
    )
    material = _get_scalar(
        bresen_df.loc[bresen_df["EQ_naam"] == simulation_name, "levee_mate"],
        "levee_mate",
        simulation_name,
    )

    if "simulation_data.csv" in simulations_data:
        csv_simulation_data = pd.read_csv(region_path / "simulation_data.csv", sep=";")
        breach_data = csv_simulation_data.iloc[-1]

        bresdiepte = float(str(breach_data["Maximum Breach Depth"]).replace(",", "."))
        maximale_bresbreedte = np.round(float(str(breach_data["Maximum Breach Width"]).replace(",", ".")), 0)
        maximaal_bresdebiet = float(str(breach_data["Maximum Breach Discharge"]).replace(",", "."))
        maximale_buitenwaterstand = float(str(breach_data["Maximum Upstream Water Level"]).replace(",", "."))
    else:
        bresdiepte = _get_scalar(
            metadata_df_ns.loc[metadata_df_ns["Scenarionaam"] == simulation_name, "Bresdiepte"],
            "Bresdiepte",
            simulation_name,
        )
        maximale_bresbreedte = _get_scalar(
            metadata_df_ns.loc[metadata_df_ns["Scenarionaam"] == simulation_name, "Maximale bresbreedte"],
            "Maximale bresbreedte",
            simulation_name,
        )
        maximaal_bresdebiet = _get_scalar(
            metadata_df_ns.loc[metadata_df_ns["Scenarionaam"] == simulation_name, "Maximaal bresdebiet"],
            "Maximaal bresdebiet",
            simulation_name,
        )
        maximale_buitenwaterstand = _get_scalar(
            metadata_df_ns.loc[metadata_df_ns["Scenarionaam"] == simulation_name, "Maximale buitenwaterstand"],
            "Maximale buitenwaterstand",
            simulation_name,
        )

    return {
        "coordinate_x": coordinate_x,
        "coordinate_y": coordinate_y,
        "naam_waterkering": naam_waterkering,
        "initial_crest_level": initial_crest_level,
        "material": material,
        "bresdiepte": bresdiepte,
        "maximale_bresbreedte": maximale_bresbreedte,
        "maximaal_bresdebiet": maximaal_bresdebiet,
        "maximale_buitenwaterstand": maximale_buitenwaterstand,
    }


def _get_simulation_info(
    api_client: V3Api,
    simulation_name: str,
    metadata_df_ns: pd.DataFrame,
    fallback_model_id: int = 75909,
) -> dict:
    simulation = api_client.usage_list(simulation__name=simulation_name)

    if len(simulation.results) != 0:
        simulation_last = simulation.results[0]
        model_id = simulation_last.simulation.threedimodel_id
        model_result = api_client.threedimodels_list(id=model_id)

        schematisation_id = model_result.results[0].schematisation_id
        revision_number = model_result.results[0].revision_number
        model_versie = f"Schematisation id {schematisation_id} - Revision #{revision_number}"

        simulation_started_raw = simulation_last.started
        scenariodatum = simulation_started_raw.strftime("%Y-%m-%d")

        simulation_start_raw = simulation_last.simulation.start_datetime
        simulation_start = simulation_start_raw.strftime("%d-%m-%y %H:%M %S")

        mod_date_raw = model_result.results[0].revision_commit_date
        mod_date = mod_date_raw.split("T")[0]

        simulation_end_raw = simulation_last.simulation.end_datetime
        simulation_end = simulation_end_raw.strftime("%d-%m-%y %H:%M %S")

        simulation_duur = simulation_end_raw - simulation_start_raw
        sim_duur = f"{simulation_duur.days} d 00:00"

        log_start_datum = simulation_last.started.strftime("%d-%m-%Y %H:%M:%S")
        log_total_time_raw = timedelta(seconds=int(simulation_last.total_time))
        log_total_time_format = datetime(1, 1, 1) + log_total_time_raw
        log_total_time = "0 d " + log_total_time_format.strftime("%H:%M")
        log_end_datum = simulation_last.finished.strftime("%d-%m-%Y %H:%M:%S")

    else:
        model_result = api_client.threedimodels_list(id=fallback_model_id)
        mod_date_raw = model_result.results[0].revision_commit_date
        mod_date = mod_date_raw.split("T")[0]

        model_versie = _get_scalar(
            metadata_df_ns.loc[metadata_df_ns["Scenarionaam"] == simulation_name, "Modelversie"],
            "Modelversie",
            simulation_name,
        )
        scenariodatum = _get_scalar(
            metadata_df_ns.loc[metadata_df_ns["Scenarionaam"] == simulation_name, "Scenariodatum"],
            "Scenariodatum",
            simulation_name,
        )
        log_start_datum = _get_scalar(
            metadata_df_ns.loc[metadata_df_ns["Scenarionaam"] == simulation_name, "Start berekening"],
            "Start berekening",
            simulation_name,
        )
        log_end_datum = _get_scalar(
            metadata_df_ns.loc[metadata_df_ns["Scenarionaam"] == simulation_name, "Einde berekening"],
            "Einde berekening",
            simulation_name,
        )
        log_total_time = _get_scalar(
            metadata_df_ns.loc[metadata_df_ns["Scenarionaam"] == simulation_name, "Rekenduur"],
            "Rekenduur",
            simulation_name,
        )
        simulation_start = _get_scalar(
            metadata_df_ns.loc[metadata_df_ns["Scenarionaam"] == simulation_name, "Start simulatie"],
            "Start simulatie",
            simulation_name,
        )
        simulation_end = _get_scalar(
            metadata_df_ns.loc[metadata_df_ns["Scenarionaam"] == simulation_name, "Einde simulatie"],
            "Einde simulatie",
            simulation_name,
        )
        sim_duur = _get_scalar(
            metadata_df_ns.loc[metadata_df_ns["Scenarionaam"] == simulation_name, "Duur"],
            "Duur",
            simulation_name,
        )

    return {
        "model_versie": model_versie,
        "mod_date": mod_date,
        "scenariodatum": scenariodatum,
        "log_start_datum": log_start_datum,
        "log_end_datum": log_end_datum,
        "log_total_time": log_total_time,
        "simulation_start": simulation_start,
        "simulation_end": simulation_end,
        "sim_duur": sim_duur,
    }


def _fill_metadata_row(
    metadata_temp: pd.DataFrame,
    simulation_name: str,
    breach_info: dict,
    simulation_info: dict,
) -> pd.DataFrame:
    initiele_bresbreedte = 10
    duur_verticale_richting = "00 d 00:10"
    methode_bresgroei = 1
    startmoment_bresgroei = "00 d 00:00"

    uc = 0.6
    f1 = 1.79
    f2 = 0.04
    ce = 1
    lowest_crest_level = breach_info["initial_crest_level"] - breach_info["bresdiepte"]

    relative_path = "results_3di.nc"
    relative_path_dem = "dem_clip.tif"

    metadata_temp.loc[len(metadata_temp), "Scenarionaam"] = simulation_name

    mask = metadata_temp["Scenarionaam"] == simulation_name

    metadata_temp.loc[mask, "Materiaal kering"] = breach_info["material"]
    metadata_temp.loc[mask, "Bresdiepte"] = breach_info["bresdiepte"]
    metadata_temp.loc[mask, "Duur bresgroei in verticale richting"] = duur_verticale_richting
    metadata_temp.loc[mask, "Initiele bresbreedte"] = initiele_bresbreedte
    metadata_temp.loc[mask, "Methode bresgroei"] = methode_bresgroei
    metadata_temp.loc[mask, "Startmoment bresgroei"] = startmoment_bresgroei
    metadata_temp.loc[mask, "Maximale bresbreedte"] = breach_info["maximale_bresbreedte"]
    metadata_temp.loc[mask, "Kritieke stroomsnelheid (Uc)"] = uc
    metadata_temp.loc[mask, "bresgroeifactor 1 (f1)"] = f1
    metadata_temp.loc[mask, "bresgroeifactor 2 (f2)"] = f2
    metadata_temp.loc[mask, "Afvoer coefficient (Ce)"] = ce
    metadata_temp.loc[mask, "Initial Crest [m+NAP]"] = breach_info["initial_crest_level"]
    metadata_temp.loc[mask, "Lowest crest"] = lowest_crest_level
    metadata_temp.loc[mask, "Maximaal bresdebiet"] = breach_info["maximaal_bresdebiet"]

    metadata_temp.loc[mask, "Ruwheid model"] = "Lizard/STOWA WSS 2021"
    metadata_temp.loc[mask, "Bodemhoogte model"] = "AHN4 2020"
    metadata_temp.loc[mask, "Naam buitenwater"] = "Boezemwater"
    metadata_temp.loc[mask, "Datum modelschematisatie"] = simulation_info["mod_date"]
    metadata_temp.loc[mask, "Start berekening"] = simulation_info["log_start_datum"]
    metadata_temp.loc[mask, "Scenariodatum"] = simulation_info["scenariodatum"]
    metadata_temp.loc[mask, "Einde berekening"] = simulation_info["log_end_datum"]
    metadata_temp.loc[mask, "Rekenduur"] = simulation_info["log_total_time"]
    metadata_temp.loc[mask, "Start simulatie"] = simulation_info["simulation_start"]
    metadata_temp.loc[mask, "Einde simulatie"] = simulation_info["simulation_end"]
    metadata_temp.loc[mask, "Duur"] = simulation_info["sim_duur"]
    metadata_temp.loc[mask, "3Di simulatie resultaat"] = relative_path
    metadata_temp.loc[mask, "Bathymetrie"] = relative_path_dem
    metadata_temp.loc[mask, "Scenario Identificatie"] = simulation_name
    metadata_temp.loc[mask, "Scenariotype"] = "C"
    metadata_temp.loc[mask, "Modelversie"] = simulation_info["model_versie"]
    metadata_temp.loc[mask, "Overschrijdingsfrequentie"] = -9999
    metadata_temp.loc[mask, "Modelleersoftware"] = "3di"
    metadata_temp.loc[mask, "Projectnaam"] = "Overstromingsberekeningen primaire doorbraken 2024."
    metadata_temp.loc[mask, "Eigenaar overstromingsinformatie"] = 3
    metadata_temp.loc[mask, "Versie resultaat"] = 1
    metadata_temp.loc[mask, "Varianttype"] = "Bres"
    metadata_temp.loc[mask, "Motivatie rekenmethode"] = (
        "Actualisatie maaiveldmodel, berekening mogelijk op hoge resolutie. "
        "Boezemsysteem in 1D gemodelleerd t.b.v. verspreiding regionaal systeem."
    )
    metadata_temp.loc[mask, "Houdbaarheid scenario"] = "5 tot 10 jaar"
    metadata_temp.loc[mask, "x-coordinaten doorbraaklocatie"] = int(breach_info["coordinate_x"])
    metadata_temp.loc[mask, "y-coordinaten doorbraaklocatie"] = int(breach_info["coordinate_y"])
    metadata_temp.loc[mask, "Naam waterkering"] = breach_info["naam_waterkering"]
    metadata_temp.loc[mask, "Buitenwatertype"] = "boezemwater"
    metadata_temp.loc[mask, "Maximale buitenwaterstand"] = breach_info["maximale_buitenwaterstand"]
    metadata_temp.loc[mask, "Herhalingstijd buitenwater"] = -9999
    metadata_temp.loc[mask, "Modelresolutie"] = "5"
    metadata_temp.loc[mask, "Regionale keringen of hoge lijnelementen standzeker"] = "ja"
    metadata_temp.loc[mask, "Berekeningsmethode"] = "2d model"
    metadata_temp.loc[mask, "Doel"] = "Evenwichtsberekening bij regionale keringen"
    metadata_temp.loc[mask, "Beschrijving scenario"] = "Doorbraak primaire waterkering."
    metadata_temp.loc[mask, "MOD_VERSIE"] = simulation_info["model_versie"]
    metadata_temp.loc[mask, "Compartimentering van de boezem"] = "nee"
    metadata_temp.loc[mask, "Gebiedsnaam"] = (
        "gebieden beschermd door genormeerde regionale keringen, langs rivieren, meren, kanalen en boezemwateren"
    )

    return metadata_temp


def generate_ldo_metadata_per_scenario(
    bresen_path: str,
    metadata_template_path: str,
    metadata_nzk_path: str,
    base_folder: str,
    metadata_per_scenario_folder: str,
    scenario_id_path: str,
    skip_scenarios: list[str] | None = None,
    fallback_model_id: int = 75909,
) -> dict:
    if skip_scenarios is None:
        skip_scenarios = []

    skip_scenarios_set = set(skip_scenarios)

    row0 = pd.read_excel(metadata_template_path, header=None, nrows=1)
    metadata_df_ns = pd.read_excel(metadata_nzk_path, sheet_name="Scenario data", header=1)
    metadata_df = pd.read_excel(metadata_template_path, header=1)
    bresen_df = gpd.read_file(bresen_path)
    scenario_id_df = pd.read_excel(scenario_id_path)

    processed_done = set(scenario_id_df["Naam van het scenario"].dropna().astype(str).tolist())

    api_client = _get_api_client()
    dl.set_api_key(api_client)

    scenario_paths = [p for p in Path(base_folder).iterdir() if p.is_dir()]

    processed = []
    skipped = []
    already_done = []
    failed = []

    Path(metadata_per_scenario_folder).mkdir(parents=True, exist_ok=True)

    for region_path in scenario_paths:
        simulation_name = region_path.name

        if simulation_name in skip_scenarios_set:
            print(f"Skipping {simulation_name}")
            skipped.append(simulation_name)
            continue

        if simulation_name in processed_done:
            print(f"Scenario {simulation_name} already done")
            already_done.append(simulation_name)
            continue

        try:
            print(f"Adding metadata to {simulation_name}")

            metadata_temp = metadata_df.copy()

            breach_info = _get_breach_info(
                simulation_name=simulation_name,
                region_path=region_path,
                metadata_df_ns=metadata_df_ns,
                bresen_df=bresen_df,
            )

            simulation_info = _get_simulation_info(
                api_client=api_client,
                simulation_name=simulation_name,
                metadata_df_ns=metadata_df_ns,
                fallback_model_id=fallback_model_id,
            )

            netcdf_folder = region_path / "01_NetCDF"
            resultnc = netcdf_folder / "results_3di.nc"
            resulth5 = netcdf_folder / "gridadmin.h5"
            GridH5ResultAdmin(str(resulth5), resultnc)

            metadata_temp = _fill_metadata_row(
                metadata_temp=metadata_temp,
                simulation_name=simulation_name,
                breach_info=breach_info,
                simulation_info=simulation_info,
            )

            row0_aligned = row0.iloc[:, : len(metadata_temp.columns)]
            metadata_output_path = Path(metadata_per_scenario_folder) / f"{simulation_name}.xlsx"

            with pd.ExcelWriter(metadata_output_path, engine="openpyxl", mode="w") as writer:
                row0_aligned.to_excel(writer, index=False, header=False, startrow=0)
                metadata_temp.to_excel(writer, index=False, header=True, startrow=1)

            scenario_id_df.loc[len(scenario_id_df), "Naam van het scenario"] = simulation_name
            scenario_id_df.to_excel(scenario_id_path, index=False)

            processed_done.add(simulation_name)
            processed.append(simulation_name)

        except Exception as exc:
            print(f"Failed for {simulation_name}: {exc}")
            traceback.print_exc()
            failed.append(simulation_name)

    return {
        "processed": processed,
        "skipped": skipped,
        "already_done": already_done,
        "failed": failed,
    }


# %%

result = generate_ldo_metadata_per_scenario(
    bresen_path=r"Y:\03.resultaten\Normering Regionale Keringen\ipo_ldo_sctructuur\bressen.shp",
    metadata_template_path=r"Y:\03.resultaten\Normering Regionale Keringen\ipo_ldo_sctructuur\import_scenarios.xlsx",
    metadata_nzk_path=r"Y:\03.resultaten\Normering Regionale Keringen\output\scenarios_output\N&S\20260217_Scenarios_in_LDO.xlsx",
    base_folder=r"\\corp.hhnk.nl\data\Hydrologen_data\Data\03.resultaten\Normering Regionale Keringen\output\scenarios_output\N&S\sbln",
    metadata_per_scenario_folder=r"Y:\03.resultaten\Normering Regionale Keringen\output\scenarios_output\N&S\ldo_structuur\metadata_per_scenario",
    scenario_id_path=r"Y:\03.resultaten\Normering Regionale Keringen\output\scenarios_output\N&S\ldo_structuur\scenarios_ids.xlsx",
    skip_scenarios=["IPO_SBMN_EQ_1632", "IPO_AB_EQ_1125"],
)
print(result)

# %%

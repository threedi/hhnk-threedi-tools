# %%

import os
import time
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
from threedigrid.admin.gridresultadmin import (
    GridH5AggregateResultAdmin,
    GridH5ResultAdmin,
)

from hhnk_threedi_tools.core.folders import Folders

bresen = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\03.resultaten\Normering Regionale Keringen\ipo_ldo_sctructuur\bressen.shp"
metadata_path = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\03.resultaten\Normering Regionale Keringen\ipo_ldo_sctructuur\import_scenarios.xlsx"
base_folder = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\03.resultaten\Normering Regionale Keringen\output\scenarios_output\N&S\sbmz"

items = os.listdir(base_folder)

metadata_df = pd.read_excel(metadata_path, header=1)
bresen_df = gpd.read_file(bresen)

API_KEY = "buufIUDN.VInEYy4hcr3afNLpNy1ZUu2Ydl86jf7b"
config = {
    "THREEDI_API_HOST": "https://api.3di.live",
    "THREEDI_API_PERSONAL_API_TOKEN": API_KEY,
}
api_client: V3Api = ThreediApi(config=config, version="v3-beta")

# Loggin Confirmation Message
try:
    user = api_client.auth_profile_list()
except ApiException:
    print("Oops, something went wrong. Maybe you made a typo?")
else:
    print(f"Successfully logged in as {user.username}!")

# SET location and name
organisation = "Hoogheemraadschap Hollands Noorderkwartier"

# SET YOUT OWN API KEY
dl.set_api_key("Ssb7LXCk.sImsUmQLjXKHsNlaDs3tU0HHzPGQD8HN")

models = []
region_paths = []
scenario_paths = []
for item in items:
    brst_path = Path(os.path.join(base_folder, item))
    if brst_path not in models:
        scenario_paths.append(brst_path)
metadata_temp = metadata_df.copy()
# %%
for region_path in scenario_paths:
    # Set model folder location

    simulation_name = region_path.name
    print(simulation_name)

    simulations_data = os.listdir(region_path)

    print(f"Adding metadata to {simulation_name}")

    # model_name = model_name_stem.split('_')[0] +'_1d2d_' + model_name_stem.split('_')[1]
    coordinate_x = bresen_df.loc[bresen_df["EQ_naam"] == simulation_name, "x-coordina"].values[0]
    coordinate_y = bresen_df.loc[bresen_df["EQ_naam"] == simulation_name, "y-coordina"].values[0]
    naam_waterkering = bresen_df.loc[bresen_df["EQ_naam"] == simulation_name, "Naam water"].values[0]
    initial_crest_level = bresen_df.loc[bresen_df["EQ_naam"] == simulation_name, "In_Cr_lvl"].values[0]
    metadata_temp.loc[len(metadata_temp), "Scenarionaam"] = simulation_name

    for simulation_data in simulations_data:
        if "simulation_data.csv" in simulation_data:
            csv_simuation_data = pd.read_csv(os.path.join(region_path, simulation_data), sep=";")

            breach_data = csv_simuation_data.iloc[-1]
            bresdiepte = float(breach_data["Maximum Breach Depth"].replace(",", "."))
            initiele_bresbreedte = 10
            duur_verticale_richting = "00 d 00:10"
            methode_bresgroei = 1
            startmoment_bresgroei = "00 d 00:00"
            maximale_bresbreedte = np.round((float(breach_data["Maximum Breach Width"].replace(",", "."))), 0)
            uc = 0.6
            f1 = 1.79
            f2 = 0.04
            ce = 1
            lowest_crest_level = initial_crest_level - bresdiepte
            maximaal_bresdebiet = float(breach_data["Maximum Breach Discharge"].replace(",", "."))
            maximale_buitenwaterstand = float(breach_data["Maximum Upstream Water Level"].replace(",", "."))

        material = bresen_df.loc[bresen_df["EQ_naam"] == simulation_name, "levee_mate"]  # Bres_metadata
        metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Materiaal kering"] = material
        metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Bresdiepte"] = bresdiepte
        metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Duur bresgroei in verticale richting"] = (
            duur_verticale_richting
        )
        metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Initiele bresbreedte"] = (
            initiele_bresbreedte
        )
        metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Methode bresgroei"] = methode_bresgroei
        metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Startmoment bresgroei"] = (
            startmoment_bresgroei
        )
        metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Maximale bresbreedte"] = (
            maximale_bresbreedte
        )
        metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Kritieke stroomsnelheid (Uc)"] = uc
        metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "bresgroeifactor 1 (f1)"] = f1
        metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "bresgroeifactor 2 (f2)"] = f2
        metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Afvoer coefficient (Ce)"] = ce
        metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Initial Crest [m+NAP]"] = (
            initial_crest_level
        )
        metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Lowest crest"] = lowest_crest_level
        metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Maximaal bresdebiet"] = (
            maximaal_bresdebiet
        )
        metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Ruwheid model"] = "Lizard/STOWA WSS 2021"
        metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Bodemhoogte model"] = "AHN4 2020"

        # netcdf primer kering
        netcdf = os.path.join(region_path, "01_NetCDF")

        if os.path.exists(netcdf):
            # SIMULATION NAME

            simulation = api_client.usage_list(simulation__name=breach_data["name"])
            simulation_last = simulation.results[0]

            name_buiten_water = " Boezemwater"
            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Naam buitenwater"] = name_buiten_water

            model_id = simulation.results[0].simulation.threedimodel_id
            model_result = api_client.threedimodels_list(id=model_id)

            schematisation_id = model_result.results[0].schematisation_id
            # FILL MODEL VERSIE

            revision_number = model_result.results[0].revision_number
            model_versie = "Schematisation id " + str(schematisation_id) + " - Revision #" + str(revision_number)
            print(model_versie)

            #
            resultnc = os.path.join(netcdf, "results_3di.nc")

            # grid_admin.h4 IPO
            resulth5 = os.path.join(netcdf, "gridadmin.h5")

            # AGGREGARED RESULTS SECUNDARY KERING

            gr = GridH5ResultAdmin(resulth5, resultnc)

            simulation_started_raw = simulation_last.started
            simuatlion_started = simulation_started_raw.strftime("%Y-%m-%d")
            simulation_start_raw = simulation_last.simulation.start_datetime
            simulation_start = simulation_start_raw.strftime("%d-%m-%y %H:%M %S")

            mod_date_raw = model_result.results[0].revision_commit_date
            mod_date = mod_date_raw.split("T")[0]

            simulation_end_raw = simulation_last.simulation.end_datetime  #'simulation_end': '2000-01-21T00:00:00Z'
            simulation_end = simulation_end_raw.strftime("%d-%m-%y %H:%M %S")
            simulation_duur = simulation_end_raw - simulation_start_raw
            sim_duur = f"{simulation_duur.days} d 00:00"

            # Day when the simulation started (human datum)
            log_start_datum = simulation_last.started.strftime("%d-%m-%Y %H:%M:%S")
            log_total_time_raw = timedelta(seconds=int((simulation_last.total_time)))
            log_total_time_format = datetime(1, 1, 1) + log_total_time_raw

            # rekenduur
            log_total_time = "0 d " + log_total_time_format.strftime("%H:%M")

            log_end_datum = simulation_last.finished.strftime("%d-%m-%Y %H:%M:%S")

            simulation_id = simulation_last.simulation.id

            # SET IPO LOCATION RELATIVE PATH
            relative_path = "results_3di.nc"
            relative_path_dem = "dem_clip.tif"

            # scenario identification
            scenario_identification = simulation_name + "_" + str(simulation_id)

            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Datum modelschematisatie"] = mod_date

            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Bodemhoogte model"] = "AHN4 2020"

            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Start berekening"] = log_start_datum
            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Scenariodatum"] = simuatlion_started
            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Einde berekening"] = log_end_datum

            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Rekenduur"] = log_total_time
            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Start simulatie"] = simulation_start
            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Einde simulatie"] = simulation_end

            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Duur"] = sim_duur

            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Simulatie resultaat"] = relative_path

            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Bathymetrie"] = relative_path_dem

            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Scenario Identificatie"] = (
                scenario_identification
            )

            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Scenariotype"] = "C"

            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Modelversie"] = (
                simulation_last.simulation.threedicore_version
            )

            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Modelleersoftware"] = "3di"

            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Projectnaam"] = (
                "Overstromingsberekeningen primaire doorbraken 2024."
            )

            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Eigenaar overstromingsinformatie"] = 3
            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Versie resultaat"] = 1
            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Varianttype"] = "Bres"

            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Motivatie rekenmethode"] = (
                "Actualisatie maaiveldmodel, berekening mogelijk op hoge resolutie. Boezemsysteem in 1D gemodelleerd t.b.v. verspreiding regionaal systeem."
            )
            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Houdbaarheid scenario"] = (
                "5 tot 10 jaar"
            )

            metadata_temp.loc[
                metadata_temp["Scenarionaam"] == simulation_name, "x-coordinaten doorbraaklocatie/effectgebied"
            ] = coordinate_x
            metadata_temp.loc[
                metadata_temp["Scenarionaam"] == simulation_name, "y-coordinaten doorbraaklocatie/effectgebied"
            ] = coordinate_y
            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Naam waterkering"] = naam_waterkering
            # metadata_temp.loc[metadata_temp['Scenarionaam'] == simulation_name, 'Naam buitenwater'] = naam_buitenwater

            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Buitenwatertype"] = "boezemwater"

            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Maximale buitenwaterstand"] = (
                maximale_buitenwaterstand
            )
            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Herhalingstijd buitenwater"] = -9999
            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Modelresolutie"] = "5"
            metadata_temp.loc[
                metadata_temp["Scenarionaam"] == simulation_name, "Regionale keringen of hoge lijnelementen standzeker"
            ] = "ja"
            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Berekeningsmethode"] = "2d model"
            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Doel"] = (
                "Evenwichtsberekening bij regionale keringen"
            )
            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Beschrijving scenario"] = (
                "Doorbraak primaire waterkering."
            )
            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "MOD_VERSIE"] = model_versie
            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Compartimentering van de boezem"] = (
                "nee"
            )
            metadata_temp.loc[metadata_temp["Scenarionaam"] == simulation_name, "Gebiedsnaam"] = (
                "gebieden beschermd door genormeerde regionale keringen, langs rivieren, meren, kanalen en boezemwateren"
            )
            # scenario_done.append(path)
# %%

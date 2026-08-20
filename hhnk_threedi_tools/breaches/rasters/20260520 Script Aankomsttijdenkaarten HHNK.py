# -*- coding: utf-8 -*-
"""
Created on Tue May  5 09:39:22 2026

@author: rick.vanbentem
"""

# %% Importeren van alle benodigde Python-bibliotheken
import os
from contextlib import ExitStack
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from rasterio.features import rasterize
from rasterio.merge import merge
from rasterio.warp import Resampling, reproject
from shapely.geometry import Polygon, box
from threedigrid.admin.gridresultadmin import GridH5ResultAdmin


# Deze functie zoekt binnen een hoofdmap naar een specifieke scenario-map.
# Dit is nodig omdat scenario’s in verschillende subfolders kunnen zitten.
def find_project_folder(root: Path, scenario_id: str) -> Path:
    for dirpath, dirnames, _ in os.walk(root):
        if scenario_id in dirnames:
            return Path(dirpath) / scenario_id
    raise FileNotFoundError(f"{scenario_id} not found in {root}")


# Deze functie zoekt één bestand met een bepaalde extensie (bijv. .tif of .nc) voor het specifieke scenario in de projectmap
def find_single_file(folder: Path, suffixes):
    for p in folder.iterdir():
        if p.is_file() and p.suffix.lower() in suffixes:
            return p
    raise FileNotFoundError(f"No file with suffix {suffixes} in {folder}")


# Geeft alle submapnamen terug binnen een map
def get_subfolder_names(folder):
    return [p.name for p in Path(folder).iterdir() if p.is_dir()]


# Controleert of een scenario bestaat in één van de opgegeven hoofdmappen
def scenario_bestaat(scenario, *root_folders):
    return any(any(p.is_dir() and p.name == scenario for p in root.rglob("*")) for root in root_folders)


# %% Variables

# Defineren van de minimale waterdiepte waarvoor output gegenereerd wordt, alles onder deze waarde wordt niet meegenomen in de output raster
MIN_DEPTH = 0.20

# Tijdstappen (in uren) waarvoor we resultaten make
times = [2, 4, 8, 12, 24, 36, 48]

# -------------------------------------------------------------
# Hier definiëren we waar alle input- en outputdata staat
# -------------------------------------------------------------

project_folder = Path(r"G:\Projecten 2026\20260083 - Aankomsttijdenkaarten bestuur HHNK\Gegevens")

dem_folder = project_folder / "Aangeleverd" / "20260429 - HHNK - Informatie bresscenario's (DEM)" / "dem_per_scenario"
nc_folder = project_folder / "data_netcdf"

# -------------------------------------------------------------
# Scenario’s worden ingelezen uit een Excelbestand
# -------------------------------------------------------------
# Excelbestand staat in de hoofdmap
df_scenarios = pd.read_excel(project_folder / "Scenarios_per_gebied.xlsx")
df_scenarios.columns = ["traject", "naam", "locatie", "scenario"]

# Overzicht trajecten
trajecten = df_scenarios.groupby("traject")["scenario"].unique().apply(list)

# Maak een leesbare naam per traject
traject_namen = (
    df_scenarios.set_index("traject")[["locatie", "naam"]]
    .stack()
    .dropna()
    .groupby(level=0)
    .unique()
    .apply(lambda x: ", ".join(x))
)

# Selecteer het traject op basis van de volgorde in het excel bestand
# Python is zero based, dus de eerste waarde is 0, de tweede waarde is 1, etc.
traject_id = 4

traject_scenarios = trajecten.iloc[traject_id]
traject_naam = traject_namen.iloc[traject_id]

# Outputmap waarin de resultaten opgeslagen worden, deze heeft de naam van het gekozen traject
base_output_folder = project_folder / "Resultaat" / traject_naam

# Map in de outputmap waar de gecombineerde resultaten in opgeslagen worden
merged_output_folder = base_output_folder / "output_merged"
merged_output_folder.mkdir(parents=True, exist_ok=True)

# Submappen voor elke tijdstap van elk scenario
for t in times:
    output_folder = base_output_folder / f"t{t:02d}h"
    output_folder.mkdir(parents=True, exist_ok=True)

# Check of de bestanden er zijn
map_ontbreekt = [s for s in traject_scenarios if not scenario_bestaat(s, dem_folder, nc_folder)]

if map_ontbreekt:
    print("Scenario’s zonder map:")
    for s in map_ontbreekt:
        print(f" - {s}")


# %%
# -------------------------------------------------------------
# HOOFDVERWERKING PER SCENARIO
# -----------------------------------------------------------

# Lege array maken om missende files in op te slaan ter controle
dem_missing = []

for scenario_id in traject_scenarios:
    # Start instance
    print(f"Start met {scenario_id}")

    # Als map ontbreekt -> overslaan
    if scenario_id in map_ontbreekt:
        print(f"Map ontbreekt voor {scenario_id}, overslaan")
        continue

    # Bepaal locaties van data
    dem_location = find_project_folder(dem_folder, scenario_id)
    nc_location = find_project_folder(nc_folder, scenario_id)

    # Bestanden ophalen
    try:
        dem_file = str(find_single_file(dem_location, {".tif", ".tiff"}))
    except FileNotFoundError:
        dem_missing.append(scenario_id)
        print(f"DEM ontbreekt voor {scenario_id}, overslaan")
        continue

    gridadmin_file = str(find_single_file(nc_location / "netcdf", {".h5"}))
    nc_file = str(find_single_file(nc_location / "netcdf", {".nc"}))

    # -------------------------------------------------------------
    # DEM CONTROLE
    # -------------------------------------------------------------

    # We controleren of:
    # - er een nodata waarde is
    # - deze correct wordt gebruikt

    with rasterio.open(dem_file) as src:
        nodata = src.nodata
        data = src.read(1, masked=True)

    print(f"DEM nodata value: {nodata}")

    if nodata is None:
        raise ValueError("DEM heeft geen nodata-waarde gedefinieerd")

    if not np.ma.is_masked(data):
        raise ValueError("DEM nodata wordt niet als mask gebruikt")

    print(f"Aantal nodata pixels: {data.mask.sum()}")
    print(f"DEM min/max (zonder nodata): {data.min()} / {data.max()}")

    # -------------------------------------------------------------
    # 3Di RESULTATEN INLEZEN
    # -------------------------------------------------------------

    gr = GridH5ResultAdmin(gridadmin_file, nc_file)
    timestamps = gr.nodes.timestamps

    # Open NetCDF met 3Di-resultaten
    ds = xr.open_dataset(nc_file)

    # Waterstand per 2D-element (tijd, element)
    waterlevel = ds["Mesh2D_s1"]

    # Maximale waterstand ooit per element (voor voorfilter)
    sumax = ds["Mesh2DFace_sumax"].values

    # Contourcoördinaten van 2D-elementen
    x = ds["Mesh2DContour_x"].values
    y = ds["Mesh2DContour_y"].values

    # Selecteer timestamps voor de gevraagde stappen
    timesteps = [t * 3600 for t in times]
    indices = [int(np.abs(timestamps - t).argmin()) for t in timesteps]

    # -------------------------------------------------------------
    # DEM VOORBEREIDEN
    # ------------------------------------------------------------

    with rasterio.open(dem_file) as dem_src:
        dem = dem_src.read(1)  # maaiveldhoogte
        dem_transform = dem_src.transform  # pixels naar kaartcoördinaten
        dem_crs = dem_src.crs  # CRS voor output
        dem_shape = dem.shape  # rastervorm (rows, cols)
        dem_nodata = dem_src.nodata  # nodata-waarde DEM
        dem_bbox = box(*dem_src.bounds)  # extent DEM als polygon

    # -------------------------------------------------------------
    # POLYGONEN VAN GRIDCELLEN MAKEN
    # ----------------------------------------------------------

    polygons = []
    poly_index = []

    for i in range(x.shape[0]):
        # sla cellen over die nooit nat zijn geweest
        if not np.isfinite(sumax[i]) or sumax[i] <= 0:
            continue

        # maak lijst van hoekpunten
        coords = list(zip(x[i], y[i]))

        # minimaal 3 unieke punten nodig
        if len(set(coords)) < 3:
            continue

        # maak polygon van cel
        poly = Polygon(coords)

        # sla ongeldige of lege polygonen over
        if not poly.is_valid or poly.area == 0:
            continue

        # sla polygonen buiten DEM-gebied over
        if not poly.intersects(dem_bbox):
            continue

        # bewaar geldige polygon en bijbehorende index
        polygons.append(poly)
        poly_index.append(i)

    # -------------------------------------------------------------
    # WATERDIEPTE BEREKENEN PER TIJDSTAP
    # -------------------------------------------------------------

    for t_hour, t_idx in zip(times, indices):
        print(f"  Tijdstap {t_hour} uur (index {t_idx})")

        # waterstand per element op tijdstip t
        wl_t = waterlevel.isel(time=t_idx).values

        # combineer polygonen met waterstand
        shapes = [(polygons[j], float(wl_t[i])) for j, i in enumerate(poly_index) if np.isfinite(wl_t[i])]

        # rasteriseer waterstand naar DEM-grid
        wl_raster = rasterize(
            shapes,
            out_shape=dem_shape,
            transform=dem_transform,
            fill=np.nan,
            dtype="float32",
            all_touched=True,
        )

        # bereken waterdiepte per pixel
        depth = wl_raster - dem

        # alles onder minimale diepte naar nan
        depth[depth <= MIN_DEPTH] = np.nan

        # DEM-nodata naar nan
        if dem_nodata is not None:
            depth[dem == dem_nodata] = np.nan

        # defineer output tif
        out_file = base_output_folder / f"t{t_hour:02d}h" / f"waterdiepte_t{t_hour:02d}h_s{scenario_id}.tif"

        # schrijf waterdiepte naar GeoTIFF
        with rasterio.open(
            out_file,
            "w",
            driver="GTiff",
            height=dem_shape[0],
            width=dem_shape[1],
            count=1,
            dtype="float32",
            crs=dem_crs,
            transform=dem_transform,
            nodata=np.nan,
            compress="deflate",
            tiled=True,
        ) as dst:
            dst.write(depth, 1)

# %%
# -------------------------------------------------------------
# SAMENVOEGEN VAN RESULTATEN
# -------------------------------------------------------------


for output_folder in sorted(base_output_folder.glob("t*h")):
    tif_files = sorted(output_folder.glob("*.tif"))
    if not tif_files:
        continue

    print(f"Combineren: {output_folder.name}")

    with ExitStack() as stack:
        # open alle rasters veilig
        srcs = [stack.enter_context(rasterio.open(tif)) for tif in tif_files]

        # check: CRS en resolutie moeten gelijk zijn
        ref = srcs[0]
        for src in srcs[1:]:
            if src.crs != ref.crs or src.res != ref.res:
                raise ValueError(f"Inconsistent raster: {src.name}")

        # merge: neem per pixel de maximale waarde
        merged, out_transform = merge(
            srcs,
            method="max",
        )

        merged = merged.astype("float32")

        # globale sanity check, check of hoogste waardes overeen komen
        max_input = max(
            np.nanmax(
                np.where(
                    src.read(1) == src.nodata,
                    np.nan,
                    src.read(1),
                )
            )
            for src in srcs
        )
        max_merged = np.nanmax(merged)
        print(f"  max input: {max_input:.2f}, max merged: {max_merged:.2f}")

        if max_merged < max_input - 1e-6:
            raise ValueError("Maximale waarde verdwenen tijdens merge")

        # schrijf merged raster
        profile = ref.profile
        profile.update(
            height=merged.shape[1],
            width=merged.shape[2],
            transform=out_transform,
            count=1,
            dtype="float32",
            nodata=np.nan,
            compress="deflate",
            tiled=True,
        )

        out_file = merged_output_folder / f"waterdiepte_max_{output_folder.name}.tif"

        with rasterio.open(out_file, "w", **profile) as dst:
            dst.write(merged[0], 1)

    # Extra kwaliteitscontrole
    with rasterio.open(out_file) as merged_src:
        merged_arr = merged_src.read(1)
        merged_transform = merged_src.transform
        merged_crs = merged_src.crs
        merged_shape = merged_arr.shape

    for tif in tif_files:
        with rasterio.open(tif) as src:
            src_arr = src.read(1).astype("float32")

            # normaliseer nodata
            if src.nodata is not None:
                src_arr[src_arr == src.nodata] = np.nan

            # projecteer input naar merged raster grid
            reproj_arr = np.full(merged_shape, np.nan, dtype="float32")

            reproject(
                source=src_arr,
                destination=reproj_arr,
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=merged_transform,
                dst_crs=merged_crs,
                resampling=Resampling.nearest,
            )

            # inhoudelijke check: globale max mag niet groter zijn dan merged
            if np.nanmax(reproj_arr) > np.nanmax(merged_arr) + 1e-6:
                raise ValueError(f"Waarden verloren bij merge: {tif.name} (tijd {output_folder.name})")

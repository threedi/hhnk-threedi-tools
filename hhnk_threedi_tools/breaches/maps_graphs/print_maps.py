import os
import shutil
import sys

# import hhnk_threedi_plugin.qgis_interaction.project as project
from pathlib import Path

project_qgis = Path(r"Y:\03.resultaten\Normering Regionale Keringen\00.scripts")
import sys

if (project_qgis) not in sys.path:
    sys.path.append(str(project_qgis))
import glob

import pandas as pd
import project as project
from PyQt5.QtCore import QSize
from PyQt5.QtGui import QPainter
from qgis.core import *
from qgis.PyQt.QtXml import QDomDocument
from qgis.utils import iface

output_path = QgsProject.instance().readPath("./")  # same as qgis project loc
# Connect to project and al print composers

projectInstance = QgsProject.instance()
layoutmanager = projectInstance.layoutManager()

# files_locations = r'E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output'
files_locations = r"Y:\03.resultaten\Normering Regionale Keringen\output"
items = os.listdir(files_locations)
items = ["IPO_VRNKWE_WIP_DONE"]  # use None to process all folders
scenario_paths = []
rows = []
skip = []
for item in items:
    regions_path = os.path.join(files_locations, item)
    if os.path.isdir(regions_path):
        scenarios = os.listdir(regions_path)
        scenarios = ["IPO_VRNK_WEST_421_WE"]  # use None to process all scenarios
        for scenario in scenarios:
            if scenario in skip:
                continue
            else:
                region_path = Path(os.path.join(regions_path, scenario))
                if region_path.exists():
                    scenario_paths.append(region_path)
    else:
        continue
# scenario_paths = [
#         r'E:\03.resultaten\Normering Regionale Keringen\output\IPO_SBLN_JA_WIP_DONE\IPO_SBLN_1867_JA',
#          r'E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output\ROR_PRI-dijktraject_13-5\ROR-PRI-BALGZANDDIJK_7_EN_BALGDIJK-T10',
#         # r'E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output\ROR_PRI-dijktraject_13-5\ROR-PRI-BALGZANDDIJK_7_EN_BALGDIJK-T100',
#         # r'E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output\ROR_PRI-dijktraject_13-5\ROR-PRI-BALGZANDDIJK_7_EN_BALGDIJK-T1000',
#         # r'E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output\ROR_PRI-dijktraject_13-5\ROR-PRI-BALGZANDDIJK_7_EN_BALGDIJK-T3000',
#         # r'E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output\ROR_PRI-dijktraject_13-5\ROR-PRI-BALGZANDDIJK_7_EN_BALGDIJK-T10000',
#         # r'E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output\ROR_PRI-dijktraject_13-5\ROR-PRI-BALGZANDDIJK_7_EN_BALGDIJK-T100000',
#     ]
for scenario_path in scenario_paths:
    breach_folder = os.path.join(scenario_path, "02_WSS")
    image_folder = os.path.join(scenario_path, "04_JPEG")
    new_grid_name = f"mask_flood.gpkg"
    new_grid_path = Path(breach_folder, new_grid_name)
    scenario_name = Path(scenario_path).name
    if (os.path.exists(scenario_path) and new_grid_path.exists()) == True:
        print(scenario_name)
        composer_name = "Breaches"
        p = project.Project()

        if "IPO" in scenario_name:
            breach_layer = p.get_layer("Breslocaties")
        else:
            breach_layer = p.get_layer("Breslocaties_PR_Kering")

        breach_features = breach_layer.getFeatures()
        for feat in breach_features:
            attrs = feat.attributeMap()

            if "IPO" in scenario_name:
                if scenario_name.split("_")[-1] == "WE" or scenario_name.split("_")[-1] == "JA":
                    scenario_name = scenario_name[0:-3]
                else:
                    scenario_name = scenario_name

                folium_image_path = r"G:/02_Werkplaatsen/06_HYD/Projecten/HKC25007 Ontsluiten Overstromingsbeelden/folium_map/png_kaarten_folium/Regionale Keringen/Schade"
                folium_image_path_v2 = r"G:/02_Werkplaatsen/06_HYD/Projecten/HKC25007 Ontsluiten Overstromingsbeelden/folium_map/png_kaarten_folium/Regionale Keringen/schade_v2"

                rows.append(
                    {
                        "sc_naam": attrs["region_shp"],
                        "kering_naam": attrs["KERING_NAA"],
                        "return_period": attrs["BUW_T"],
                        "scenario_naam": attrs["SC_NAAM"],
                    }
                )
                png_name_schade = f"{scenario_name}_damage.png"
                png_name_schade_v2 = f"{scenario_name}_damage_v2.jpg"
                destination = os.path.normpath(os.path.join(folium_image_path, png_name_schade))

            else:
                folium_image_path = r"G:/02_Werkplaatsen/06_HYD/Projecten/HKC25007 Ontsluiten Overstromingsbeelden/folium_map/png_kaarten_folium/Primaire Keringen/schade"
                folium_image_path_v2 = r"G:/02_Werkplaatsen/06_HYD/Projecten/HKC25007 Ontsluiten Overstromingsbeelden/folium_map/png_kaarten_folium/Primaire Keringen/schade_v2"
                folium_name = f"schade_{scenario_name}.png"
                destination = os.path.join(folium_image_path, folium_name)
                rows.append(
                    {
                        "sc_naam": attrs["SC_NAAM"],
                        "kering_naam": attrs["KERING_NAA"],
                        "return_period": attrs["BUW_T"],
                        "scenario_naam": attrs["SC_NAAM"],
                    }
                )
                png_name_schade = f"schade_{scenario_name}.png"
                png_name_schade_v2 = f"schade_{scenario_name}_v2.jpg"

            output_pdf_file = os.path.join(image_folder, png_name_schade)
        # Create DataFrame once
        features_df = pd.DataFrame(rows)

        # Filter for the specific scenario
        filter_region_df = features_df.loc[
            features_df.sc_naam == scenario_name,
            [
                "sc_naam",
                "kering_naam",
                "return_period",
                "scenario_naam",
            ],
        ]

        # Extract values if the filtered DataFrame is not empty
        if not filter_region_df.empty:
            dijk_name = (filter_region_df["kering_naam"].values)[0].split(" ")[-1]
            return_period = (filter_region_df["return_period"].values)[0]
            sc_naam = (filter_region_df["sc_naam"].values)[0]

        title = f"Totale schade bij Scenario {sc_naam} van dijk traject {dijk_name}. Terugkeerperiode (jaren): {return_period}"

        if os.path.exists(output_pdf_file):
            os.remove(output_pdf_file)
        if os.path.exists(output_pdf_file):
            print(f"the map (PNG) for the scenario {scenario_name} _{id} Already exists")
            continue
        else:
            rasters = glob.glob(os.path.join(breach_folder, "damage_orig_lizard*.tif"))
            # schade_raster_name = f"damage_orig_lizard.tif"
            schade_raster_path = Path(rasters[0])

            group_lst = ["max_schade"]

            layer = project.Layer(
                source_path=str(schade_raster_path),
                layer_name=(schade_raster_path).name,
                type="raster",
                style_path=r"Y:\03.resultaten\Normering Regionale Keringen\00.scripts\damage_lizard_v2.qml",
                subject="",
                group_lst=group_lst,
            )
            p.add_layer(layer=layer, group_lst=group_lst, visible=True)

            new_grid_name = f"mask_flood.gpkg"
            new_grid_path = Path(breach_folder, new_grid_name)
            group_grid = ["new_grid"]

            layer_grid = project.Layer(
                source_path=str(new_grid_path),
                layer_name=new_grid_path.stem,
                type="vector",
                style_path=r"Y:\03.resultaten\Normering Regionale Keringen\00.scripts\new_grid.qml",
                subject="",
                group_lst=group_grid,
            )
            p.add_layer(layer=layer_grid, group_lst=group_grid, visible=True)
            if "IPO" in scenario_name:
                breach_layer.setSubsetString(f" \"region_shp\" = '{scenario_name}'")
            else:
                breach_layer.setSubsetString(f" \"SC_NAAM\" = '{scenario_name}'")
            layout_item = layoutmanager.layoutByName(composer_name)  # test is the layout name

            # -------------------------------------------------------------------------------------
            # Change layout settings
            # -------------------------------------------------------------------------------------
            label_item = layout_item.itemById("titel")
            label_item.setText(title)

            picture_item = layout_item.itemById("flood_picture")

            #            picture_item.setPicturePath(os.path.join(image_folder, f"{scenario}_agg.png"))
            for bar_graph in os.listdir(image_folder):
                if "bar_graph" in bar_graph:
                    bar_graph_path = os.path.join(image_folder, bar_graph)

            picture_item.setPicturePath(bar_graph_path)

            map = layout_item.referenceMap()

            raster = p.get_layer(new_grid_path.stem)
            extent = raster.extent()
            iface.mapCanvas().setExtent(extent)
            canvas = qgis.utils.iface.mapCanvas()
            map.zoomToExtent(canvas.extent())
            pdf_settings = QgsLayoutExporter.ImageExportSettings()
            pdf_settings.dpi = 400
            pdf_settings.textRenderFormat = QgsRenderContext.TextFormatAlwaysText

            map.zoomToExtent(canvas.extent())

            pdf_settings = QgsLayoutExporter.ImageExportSettings()
            pdf_settings.dpi = 400
            pdf_settings.textRenderFormat = (
                QgsRenderContext.TextFormatAlwaysText
            )  # If not changed the labels will be ugly in the pdf

            image_settings = QgsLayoutExporter.ImageExportSettings()

            export = QgsLayoutExporter(layout_item)
            # result = export.exportToPdf(output_pdf_file, pdf_settings)
            result = export.exportToImage(output_pdf_file, pdf_settings)
            if result == 0:
                print("pdf aangemaakt: {}".format(output_pdf_file))
            else:
                print("pdf niet gelukt: {}".format(output_pdf_file))

            p.remove_layer(layer_name=schade_raster_path.name, group_lst=group_lst)
            p.remove_layer(layer_name=new_grid_path.stem, group_lst=group_grid)
            breach_layer.setSubsetString("")
            breach_layer.selectByExpression("")

            if os.path.exists(destination):
                os.remove(destination)

            shutil.copyfile(output_pdf_file, destination)

            remove_png_v2_path = os.path.normpath(os.path.join(folium_image_path_v2, png_name_schade_v2))
            if os.path.exists(remove_png_v2_path):
                os.remove(remove_png_v2_path)

    else:
        print(f"the scenario {scenario} does not have geopackge file")

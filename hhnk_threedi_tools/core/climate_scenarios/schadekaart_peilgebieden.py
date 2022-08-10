import numpy as np
import multiprocessing as mp

# Third-party imports
import shapely.geometry
import geopandas as gpd
from osgeo import gdal

# Local imports
import hhnk_research_tools as hrt
import hhnk_threedi_tools as htt
from hhnk_threedi_tools.core.climate_scenarios.processes import multiprocess
from tqdm.notebook import trange, tqdm


def maak_schade_polygon(
    peilgebiedenbestand,
    schade_raster_file,
    pixel_factor,
    output_schade_file,
    output_polder_file,
):
    """Aggregeer de schaderasters van overlast en plasvorming naar peilgebiedniveau."""

    def bereken_schade_per_peilgebied(
        peilgebiedenbestand, schade_raster_file, pixel_factor
    ):
        """"""
        # importeer schaderasters
        contante_schade = {}
        for schade_type in ["overlast", "plas"]:
            raster = hrt.Raster(schade_raster_file[schade_type])
            nodata = raster.nodata
            meta = raster.metadata
            contante_schade[schade_type] = raster.get_array()
            contante_schade[schade_type][contante_schade[schade_type] == nodata] = 0

        # Importeer peilgebieden
        # gaf een fout: TopologyException: Input geom 0 is invalid: Too few points in geometry component
        # at or near point 103946.02710008621 502783.65950012207 at 103946.02710008621 502783.65950012207
        # Op deze coordinaten zat een heel klein polygon zonder peil en met opmerking dat het een heel kleine polygon was, deze is verwijderd
        # dit kan vaker voorkomen doordat er fouten zitten in de peilgebieden bestanden
        # oplossing: coordinaten in GIS opzoeken en fouten lijnen of hele kleine peilgebieden verwijderen
        peilgebieden = gpd.read_file(peilgebiedenbestand)
        # Selecteer de peilgebieden die intersecten met de bounding box van de rasters
        boundingbox = shapely.geometry.box(*meta["bounds"])
        peilgebieden = peilgebieden.loc[peilgebieden.intersects(boundingbox)]

        # Lus door de peilgebieden
        for i in trange(len(peilgebieden)):
            idx = peilgebieden.index[i]
            row = peilgebieden.iloc[i]

            # Maak een masker per polygon
            try:
                poly = row.geometry.intersection(boundingbox)
            except:
                poly = shapely.geometry.Polygon(row.geometry.exterior).intersection(
                    boundingbox
                )

            poly_gdf = gpd.GeoDataFrame(geometry=[poly])
            # Voeg kolom toe aan gdf, deze waarden worden in het raster gezet.
            poly_gdf["val"] = 1

            mask = hrt.gdf_to_raster(
                poly_gdf,
                value_field="val",
                raster_out="",
                nodata=0,
                metadata=meta,
                epsg=28992,
                driver="MEM",
            )
            # Voeg de schade toe aan de peilgebieden shape
            for schade_type in ["overlast", "plas"]:
                peilgebieden.at[idx, "cw_schade_{}".format(schade_type)] = (
                    np.nansum(contante_schade[schade_type][mask > 0]) * pixel_factor
                )
        return peilgebieden

    # Bereken de schade per peilgebied
    peilgebieden = bereken_schade_per_peilgebied(
        peilgebiedenbestand, schade_raster_file, pixel_factor
    )

    # Tabel opschonen
    schades = peilgebieden[
        [
            "id",
            "peil_id",
            "code",
            "name",
            "cw_schade_overlast",
            "cw_schade_plas",
            "geometry",
        ]
    ].sort_values(by="cw_schade_overlast", ascending=False)

    schades.columns = [
        i.replace("schade_overlast", "ws").replace("schade_plas", "mv")
        for i in schades.columns
    ]
    schades["cw_tot"] = schades["cw_ws"] + schades["cw_mv"]

    schades = schades.loc[schades["cw_tot"] > 0.0]

    schade_per_polder = (
        schades[["name", "cw_tot", "cw_ws", "cw_mv"]]
        .groupby("name")
        .sum()
        .sort_values(by="cw_ws", ascending=False)
    )

    # Opslaan naar shapefile en csv
    schades.to_file(output_schade_file)
    schade_per_polder.to_csv(output_polder_file)
    return schades, schade_per_polder


def bereken_schade_per_peilgebied_parallel(idx, row, pixel_factor, meta, boundingbox):
    """sommeer de schadewaarden per peilgebied"""
    #     # Maak een masker per polygon
    #     try:
    #         poly = row.geometry.intersection(boundingbox)
    #     except:
    #         poly = shapely.geometry.Polygon(row.geometry.exterior).intersection(boundingbox)

    #     poly_gdf = gpd.GeoDataFrame(geometry=[poly])
    #     #Voeg kolom toe aan gdf, deze waarden worden in het raster gezet.
    #     poly_gdf['val']=1

    #     print('{} peilgebied_raster_maken'.format(idx))
    #     #Maak raster masker van het geselecteerde peilgebied.
    #     mask_peilgebied = wsa.polygon_to_raster(polygon_gdf=poly_gdf, valuefield='val', raster_output_path='', nodata=0,
    #                                  meta=meta, epsg=28992, driver='MEM', datatype=gdal.GDT_Byte)

    # bereken gesommeerde schade per peilgebied
    schade_peilgebied = {}
    for schade_type in ["overlast", "plas"]:
        schade_peilgebied[schade_type] = (
            np.nansum(contante_schade[schade_type][peilgebied_array == idx])
            * pixel_factor
        )
    return (idx, schade_peilgebied)


def maak_schade_polygon_parallel(
    peilgebiedenbestand,
    schade_raster_file,
    pixel_factor,
    output_schade_file,
    output_polder_file,
):
    """Aggregeer de schaderasters van overlast en plasvorming naar peilgebiedniveau."""

    # --------------------------------------------------------------
    # importeer schaderasters
    global contante_schade
    contante_schade = {}
    for schade_type in ["overlast", "plas"]:
        raster = hrt.Raster(schade_raster_file[schade_type])
        nodata = raster.nodata
        meta = raster.metadata
        contante_schade[schade_type] = raster.get_array()
        contante_schade[schade_type][contante_schade[schade_type] == nodata] = 0

    # Importeer peilgebieden
    # gaf een fout: TopologyException: Input geom 0 is invalid: Too few points in geometry component
    # at or near point 103946.02710008621 502783.65950012207 at 103946.02710008621 502783.65950012207
    # Op deze coordinaten zat een heel klein polygon zonder peil en met opmerking dat het een heel kleine polygon was, deze is verwijderd
    # dit kan vaker voorkomen doordat er fouten zitten in de peilgebieden bestanden
    # oplossing: coordinaten in GIS opzoeken en fouten lijnen of hele kleine peilgebieden verwijderen
    peilgebieden = gpd.read_file(peilgebiedenbestand)
    peilgebieden["geometry"] = peilgebieden.buffer(0)

    # Selecteer de peilgebieden die intersecten met de bounding box van de rasters
    boundingbox = shapely.geometry.box(*meta["bounds"])
    peilgebieden = peilgebieden.loc[peilgebieden.intersects(boundingbox)]

    global peilgebied_array
    # rasterize peilgebieden
    peilgebieden["val"] = peilgebieden.index
    peilgebied_array = hrt.gdf_to_raster(
        peilgebieden,
        value_field="val",
        raster_out="",
        nodata=255,
        metadata=meta,
        epsg=28992,
        driver="MEM",
        datatype=gdal.GDT_Int16,
    )

    # #Bereken de schade per peilgebied
    results = multiprocess(
        df=peilgebieden,
        target_function=bereken_schade_per_peilgebied_parallel,
        processes=mp.cpu_count() - 1,
        pixel_factor=pixel_factor,
        meta=meta,
        boundingbox=boundingbox,
    )
    # De resulaten staan in random volgorde van berekenen. Omdat per resultaat ook de index meegegeven is, kan dit
    # terug gezet worden in de originele gdf
    peilgebied_schade = peilgebieden.copy()
    for idx, schade_peilgebied in results:
        for schade_type in ["overlast", "plas"]:
            peilgebied_schade.loc[
                idx, "cw_schade_{}".format(schade_type)
            ] = schade_peilgebied[schade_type]

    # #Tabel opschonen
    schades = peilgebied_schade[
        [
            "id",
            "peil_id",
            "code",
            "name",
            "cw_schade_overlast",
            "cw_schade_plas",
            "geometry",
        ]
    ].sort_values(by="cw_schade_overlast", ascending=False)

    schades.columns = [
        i.replace("schade_overlast", "ws").replace("schade_plas", "mv")
        for i in schades.columns
    ]
    schades["cw_tot"] = schades["cw_ws"] + schades["cw_mv"]

    schades = schades.loc[schades["cw_tot"] > 0.0]

    schade_per_polder = (
        schades[["name", "cw_tot", "cw_ws", "cw_mv"]]
        .groupby("name")
        .sum()
        .sort_values(by="cw_ws", ascending=False)
    )

    # Opslaan naar shapefile en csv
    schades.to_file(output_schade_file)
    schade_per_polder.to_csv(output_polder_file)

    del contante_schade

    return schades, schade_per_polder

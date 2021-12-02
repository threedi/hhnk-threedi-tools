import numpy as np
import shapely.geometry
import geopandas as gpd
import functions.wsa_tools as wsa  # general tools used across all scripts
from tqdm import tqdm
import multiprocessing as mp
from osgeo import gdal


def maak_schadekaart(
    schade_rasters,
    frequenties,
    output_file,
    dv,
    n,
    output_nodata,
    pixel_factor,
    damage_meta,
    damage_nodata,
):
    """Maak de jaarlijkse schade contant door gebruik te maken van een discontovoet (dv) en investeringstermijn (n)
    Schades kleiner dan 1e-5 per pixel worden op 0 gezet. Dit heeft geen significant effect op de totale schade (tot enkele tientallen euros per peilgebied). Wel een aanzienlijk effect op de bestandsgrootte.
    """

    def bereken_jaarlijkse_schade(
        schade_rasters, frequenties, damage_meta, pixel_factor
    ):
        """Om de jaarlijkse schade te berekenen openen we alle rasters en sommeren de schadewaarden.
        De schade in de rasters heeft de eenheid €/0.25m2. Omdat onze pixelgrootte 4m2 vermenigvuldigen
        we het resultaat met 16. Zo komen we weer tot de juiste waarden.
        De jaarlijkse schade wordt berekend door de schade voor een scenario te vermenigvuldigen met
        de jaarlijkse frequentie van dat scenario."""

        # Initialiseer lege arrays
        jaarlijkse_schade = np.zeros(damage_meta["shape"])
        schade = []  # Schade per inputraster

        # Bepaal de totale schade per scenario
        with tqdm(total=len(schade_rasters), unit="peilgebied") as pbar:
            for raster_file, freq in zip(schade_rasters, frequenties):
                # Open het raster met de totale schade
                damage_array = None
                damage_array, damage_nodata, damage_meta = wsa.gdal_load_raster(
                    raster_file
                )

                # Filtering ongewenste cijfers
                damage_array[damage_array < 0] = 0.0
                damage_array[
                    damage_array == damage_nodata
                ] = 0.0  # volgende keer proberen
                #             extra_nodata = np.max(damage_array) # LET OP DEZE MISSCHIEN WEER TOEVOEGEN
                #             damage_array[damage_array>=extra_nodata] = 0.0

                # Tel op bij raster
                jaarlijkse_schade += damage_array * freq

                # Voeg toe aan dataframe met simulaties
                #                 schade += [damage_array.sum() / 1e6 * pixel_factor]
                schade = 0  # dummy, bovenstaand is mogelijk traag.
                pbar.update()
            # Zet de waarden kleiner dan 0, op nan
        jaarlijkse_schade[jaarlijkse_schade <= 1e-5] = np.nan
        return jaarlijkse_schade, schade

    # Bereken jaarlijkse schade door alle 18 gebeurtenissen te gebruiken
    jaarlijkse_schade, schade = bereken_jaarlijkse_schade(
        schade_rasters, frequenties, damage_meta, pixel_factor
    )

    # Maak de jaarlijkse schade contant door gebruik te maken van een discontovoet en investeringstermijn
    cw_factor = (1 - (1 - dv) ** n) / dv

    # bereken de contante waarde van de schade
    jaarlijkse_schade_cw = jaarlijkse_schade * cw_factor
    jaarlijkse_schade_cw[jaarlijkse_schade_cw == damage_nodata] == output_nodata

    # Wegschrijven naar raster
    wsa.save_raster_array_to_tiff(
        output_file=output_file,
        raster_array=jaarlijkse_schade_cw,
        meta=damage_meta,
        nodata=output_nodata,
    )
    print("opslaan klaar")
    print(
        "De contante waarde van de schade is {:.3f} miljoen euro.".format(
            np.nansum(jaarlijkse_schade_cw[jaarlijkse_schade_cw != output_nodata])
            * pixel_factor
            / 1e6
        )
    )
    return schade


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
            contante_schade[schade_type], nodata, meta = wsa.gdal_load_raster(
                schade_raster_file[schade_type]
            )
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
        for i in tnrange(len(peilgebieden)):
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

            mask = wsa.polygon_to_raster(
                polygon_gdf=poly_gdf,
                valuefield="val",
                raster_output_path="",
                nodata=0,
                meta=meta,
                epsg=28992,
                driver="MEM",
            )
            #             mask = geometry_to_raster_mask(poly, extent=meta['bounds'], cellsize=meta['pixel_width'], shape=meta['shape'])

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
    for schade_type in schade_raster_file:
        contante_schade[schade_type], nodata, meta = wsa.gdal_load_raster(
            schade_raster_file[schade_type]
        )
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
    peilgebied_array = wsa.polygon_to_raster(
        polygon_gdf=peilgebieden,
        valuefield="val",
        raster_output_path="",
        nodata=255,
        meta=meta,
        epsg=28992,
        driver="MEM",
        datatype=gdal.GDT_Int16,
    )

    # #Bereken de schade per peilgebied
    results = wsa.multiprocess(
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



import numpy as np

import hhnk_threedi_tools.utils.notebooks.rasterclass as rasterclass #TODO to hrt.
import hhnk_research_tools as hrt

def stack_raster_arrays(raster_classes, window):
    """Inladen schade van alle 18 schade rasters voor het gegeven window"""
    stacked_array = []
    for raster in raster_classes:
        raster_array=raster._read_array(window=window)

        raster_array[raster_array<0] = 0.0
        raster_array[raster_array == raster.nodata] = 0.0

        stacked_array.append(raster_array)
        del raster_array
        
    # Stack the arrays in a 3D array.
    stacked_array = np.stack(stacked_array)
    return stacked_array


def bereken_contante_schade_window(idx, part, raster_classes, damage_raster, frequencies, cw_factor, output_nodata):
    """Om de jaarlijkse schade te berekenen openen we alle rasters en sommeren de schadewaarden. 

    De jaarlijkse schade wordt berekend door de schade voor een scenario te vermenigvuldigen met 
    de jaarlijkse frequentie van dat scenario."""
    # Retrieve window
    window=part['window']

    # Load raster
    stacked_raster_array = stack_raster_arrays(raster_classes, window=window) #laad 18 resultaten en zet in een array


    #Bereken jaarlijkse schade
    stacked_raster_array = stacked_raster_array * frequencies[:,None, None]

    stacked_raster_array = stacked_raster_array.sum(axis=0)
    stacked_raster_array[stacked_raster_array <= 1e-5] = 0

    #Maak de jaarlijkse schade contant door gebruik te maken van een discontovoet en investeringstermijn
    stacked_raster_array = stacked_raster_array * cw_factor

    return (part, stacked_raster_array)   


def main_maak_schadekaart(output_raster, schade_rasters, frequencies, output_nodata, dv, n, min_blocksize=1000):
    """Maak de jaarlijkse schade contant door gebruik te maken van een discontovoet (dv) en investeringstermijn (n)
    Schades kleiner dan 1e-5 per pixel worden op 0 gezet. Dit heeft geen significant effect op de totale schade (tot enkele tientallen euros per peilgebied). Wel een aanzienlijk effect op de bestandsgrootte.
    """
    #Inladen rasters als class
    raster_classes = [rasterclass.Raster(r) for r in schade_rasters]

    damage_raster =  raster_classes[0]
    parts = damage_raster.generate_blocks(min_blocksize=min_blocksize)
    array_out = np.ones([damage_raster.shape[0],damage_raster.shape[1]])*output_nodata

    cw_factor = (1 - (1 - dv) ** n) / dv


    # #Loop over windows and calculate results
    for idx, part in parts.iterrows():
        part, schade_window_array = bereken_contante_schade_window(idx=idx,
                                                                    part=part,
                                                                    raster_classes=raster_classes,
                                                                    damage_raster=damage_raster,
                                                                    frequencies=frequencies,
                                                                    cw_factor=cw_factor,
                                                                    output_nodata=output_nodata)

        array_out[part.window[1]:part.window[3], part.window[0]:part.window[2]] = schade_window_array


    hrt.save_raster_array_to_tiff(output_file=output_raster.path,
                                    raster_array=array_out, 
                                    nodata=output_nodata, 
                                    metadata=damage_raster.metadata)
    # print('De contante waarde van de schade is {:.3f} miljoen euro.'.format(np.nansum(jaarlijkse_schade_cw[jaarlijkse_schade_cw != output_nodata]) * pixel_factor /1e6))

# -*- coding: utf-8 -*-
"""
Created on Thu Feb  3 11:01:55 2022

@author: chris.kerklaan

-- Interpoleert in plaats van rasters de waterstanden.
    
"""

# system appends
import sys
sys.path.append(r"C:\Users\chris.kerklaan\Documents\Github\hhnk-research-tools")
sys.path.append(r"C:\Users\chris.kerklaan\Documents\Github\hhnk-threedi-tools")

# First-party imports
import numpy as np

# Third-party imports
from hhnk_threedi_tools.core.climate_scenarios.interpolate_rasters import interpoleer_deel

import hhnk_threedi_tools as htt
import threedi_raster_edits as tre

# Globals
FREQUENTIES = {
    "piek glg T10":1.75136554e-02,
    "piek glg T100":8.31066081e-03,
    "piek glg T1000":3.03734207e-04,
    "piek ggg T10":5.28657954e-02,
    "piek ggg T100":2.49891789e-02,
    "piek ggg T1000":9.11558900e-04,
    "piek ghg T10":1.08276428e-04,
    "piek ghg T100": 1.90654911e-05,
    "piek ghg T1000":1.18759960e-07,
    "blok glg T10":1.78396819e-02,
    "blok glg T100":7.58368869e-03,
    "blok glg T1000":2.43607001e-04,
    "blok ggg T10":6.45737669e-02,
    "blok ggg T100":2.61543356e-02,
    "blok ggg T1000":7.88968618e-04,
    "blok ghg T10":3.68490712e-03,
    "blok ghg T100":1.13442319e-03,
    "blok ghg T1000":1.93825386e-05,
    }


# Input
folder_path = r"C:\Users\chris.kerklaan\Documents\Projecten\hhnk_detachering\katvoed\23_Katvoed"
batch_scenario = 'katvoed #10 batch maatregel'

# Helper functions
def _grid_geometry(grid):
    """returns an ogr Datasource with the cells of a threedigrid"""
    grid.nodes.to_shape("/vsimem/nodes.shp")
    vector = tre.Vector("/vsimem/nodes.shp")
    return vector

def interpolate_levels(results:htt.Folders, recurrences, output_file):
    
    nodes = _grid_geometry(results[0].admin)

    # match the frequenties to the results
    matched = {}
    for frequentie in FREQUENTIES:
        for result in results:
            if all(n in result.name for n in frequentie.split(" ")):
                matched[result.name] = FREQUENTIES[frequentie]
    
    # stack the filtered results in an array
    levels = []
    for match in matched:
        result = results[match] 
        result_nodes = result.grid.nodes
        max_level = np.ones((1, result_nodes.s1.shape[1])) * -10000
        for i in tre.Progress(range(len(result_nodes.timestamps)), f"Retrieving levels for {match}"):
            max_level = np.maximum(result_nodes.timeseries(indexes=[i]).s1, max_level)
            
        levels.append(max_level)
    stacked = np.stack(levels)
    
    output = {}
    for recurrence in recurrences:
        output[recurrence] = interpoleer_deel(1/recurrence, stacked, 
                                        np.array(list(FREQUENTIES.values())), 
                                        min_value=None)
        
    group = tre.VectorGroup.from_scratch("Geinterpoleerde waterstanden")
        
    for recurrence, interpolated in output.items():
        output_nodes = nodes.copy()
        output_nodes.add_field("level", float)
        output_nodes.delete_field("con_nod", "con_nod_pk")
        for level, node in zip(interpolated[0], output_nodes):
            node['level'] = level
        
        group.add(output_nodes, f"T{recurrence}")
        
    group.write(output_file)
    

if __name__ == "__main__":
    folder = htt.Folders(folder_path)
    results = folder.threedi_results.one_d_two_d
    output_file = r"C:\Users\chris.kerklaan\Documents\Projecten\hhnk_detachering\katvoed/geinterpoleerde_waterstanden.gpkg"
    recurrences = [10,25,100]

    interpolate_levels(results, recurrences, output_file)
    
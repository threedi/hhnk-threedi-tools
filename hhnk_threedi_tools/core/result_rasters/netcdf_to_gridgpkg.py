# %%
from shapely.geometry import box
import numpy as np
import geopandas as gpd
import pandas as pd

class ThreediGrid:
    def __init__(self, folder, threedi_result,):
        """threedi_result : htt.core.folders.ThreediResult instance.
        
        grid creation requires:
            folder.source_data.damo
        folder.source_data.panden

        """

        self.folder=folder
        self.threedi_result = threedi_result

        self.gpkg_raw_path = self.threedi_result.pl/"grid_raw.gpkg"
        self.gpkg_corr_path = self.gpkg_raw_path.with_stem("grid_corr")


    @property
    def grid(self):
        return self.threedi_result.grid


    def netcdf_to_grid_gpkg(self, replace_dem_below_perc=50, replace_water_above_perc=95, replace_pand_above_perc=99):
        """
        ignore_dem_perc : if cell has no dem above this value waterlevels will be replaced
        ignore_water_perc : if cell has water surface area above this value waterlevels will be replaced

        create gpkg of grid with maximum wlvl
        """

        if self.gpkg_raw_path.exists():
            print(f"{self.gpkg_raw_path} already exists")
            return

        #Check required files
        if not self.folder.source_data.damo.exists:
            raise Exception(f"{self.folder.source_data.damo} - doesnt exist")
        if not self.folder.source_data.panden.exists:
            raise Exception(f"{self.folder.source_data.panden} - doesnt exist")

        grid_gdf = gpd.GeoDataFrame()

        s1_all = self.grid.nodes.subset("2D_open_water").timeseries(indexes=slice(0, -1)).s1
        vol_all = self.grid.nodes.subset("2D_open_water").timeseries(indexes=slice(0, -1)).vol

        #Find index of max wlvl value in timeseries
        s1_max_ind = s1_all.argmax(axis=0)

        # * inputs every element from row as a new function argument.
        grid_gdf["geometry"] = [box(*row) for row in self.grid.nodes.subset("2D_ALL").cell_coords.T]
        grid_gdf.crs = "EPSG:28992"
        # nodes_2d["geometry"] = [Point(*row) for row in gr.nodes.subset("2D_ALL").coordinates.T] #centerpoints.


        grid_gdf["id"] = self.grid.cells.subset("2D_open_water").id

        #Retrieve values when wlvl is max
        grid_gdf["wlvl_max_orig"] = np.round([row[s1_max_ind[enum]] for enum, row in enumerate(s1_all.T)], 5)
        grid_gdf["vol_netcdf_m3"] = np.round([row[s1_max_ind[enum]] for enum, row in enumerate(vol_all.T)], 5)


        #Percentage of dem in a calculation cell
        #so we can make a selection of cells on model edge that need to be ignored
        grid_gdf["dem_area"] = self.grid.cells.subset("2D_open_water").sumax
        #Percentage dem in calculation cell
        grid_gdf["dem_perc"] = grid_gdf["dem_area"]  / grid_gdf.area *100 



        #Check water surface area in a cell.
        water_gdf = self.folder.source_data.damo.load(layer="Waterdeel")

        water_gdf["water"] = 1
        water_cell = gpd.overlay(grid_gdf[["id", "geometry"]], water_gdf[["water", "geometry"]], how="union")
        #Select only areas with the merged feature.
        water_cell = water_cell[water_cell["water"]==1]

        #Calculate sum of area per cell
        water_cell["water_area"] = water_cell.area
        water_cell_area = water_cell.groupby("id").agg("sum")

        grid_gdf = pd.merge(grid_gdf, water_cell_area["water_area"], left_on="id", right_on="id", how="left")
        grid_gdf["water_perc"] = grid_gdf["water_area"]  / grid_gdf.area *100


        #Check building area in a cell
        pand_gdf = self.folder.source_data.panden.load(layer="panden")

        pand_gdf["pand"] = 1
        pand_cell = gpd.overlay(grid_gdf[["id", "geometry"]], pand_gdf[["pand", "geometry"]], how="union")
        #Select only areas with the merged feature.
        pand_cell = pand_cell[pand_cell["pand"]==1]

        #Calculate sum of area per cell
        pand_cell["pand_area"] = pand_cell.area
        pand_cell_area = pand_cell.groupby("id").agg("sum")

        grid_gdf = pd.merge(grid_gdf, pand_cell_area["pand_area"], left_on="id", right_on="id", how="left")
        grid_gdf["pand_perc"] = grid_gdf["pand_area"]  / grid_gdf.area *100



        #Select cells that need replacing of wlvl
        grid_gdf["replace_dem"] = grid_gdf["dem_perc"] < replace_dem_below_perc
        grid_gdf["replace_water"] = grid_gdf["water_perc"] > replace_water_above_perc
        grid_gdf["replace_pand"] = grid_gdf["pand_perc"] > replace_pand_above_perc


        grid_gdf["replace_all"] = 0
        grid_gdf.loc[grid_gdf["replace_dem"]==True, "replace_all"] = "dem"
        grid_gdf.loc[grid_gdf["replace_water"]==True, "replace_all"] = "water"
        grid_gdf.loc[grid_gdf["replace_pand"]==True, "replace_all"] = "pand"


        #grid_gdf["replace_all"] = grid_gdf["replace_dem"] | grid_gdf["replace_water"] | grid_gdf["replace_pand"]

        grid_gdf=gpd.GeoDataFrame(grid_gdf, geometry="geometry")


        #Save to file
        grid_gdf.to_file(self.gpkg_raw_path, driver="GPKG")


    def waterlevel_correction(self, output_col):

        if self.gpkg_corr_path.exists():
            print(f"{self.gpkg_corr_path} already exists")
            # return

        grid_gdf = gpd.read_file(self.gpkg_raw_path, driver="GPKG")
        
        grid_gdf[output_col] = grid_gdf["wlvl_max_orig"]
        replace_idx = grid_gdf["replace_all"]!= '0'
        grid_gdf.loc[replace_idx, output_col] = None #set values to none so they are not used in calculation of new values.

        for idx, row in grid_gdf.loc[replace_idx].iterrows():


            #Find neighbour cells
            neighbours_idx = grid_gdf[grid_gdf.geometry.touches(row.geometry)].index.tolist()
            neighbours_id = [grid_gdf.loc[neighbour_idx].id for neighbour_idx in neighbours_idx if idx != neighbour_idx]
            grid_gdf.at[idx, "neighbours"] = str(neighbours_id)


            neighbour_avg_wlvl = np.round(grid_gdf.loc[neighbours_idx][output_col].mean(), 5)
            grid_gdf.at[idx, output_col] = neighbour_avg_wlvl


        grid_gdf["diff"] = grid_gdf[output_col] - grid_gdf["wlvl_max_orig"]

        #Save to file
        grid_gdf.to_file(self.gpkg_corr_path, driver="GPKG")



if __name__ == "__main__":

    from hhnk_threedi_tools import Folders
    folder_path = r"E:\02.modellen\23_Katvoed"
    folder = Folders(folder_path)

    threedi_result = folder.threedi_results.one_d_two_d['ghg_blok_t1000']


    self = ThreediGrid(folder=folder, threedi_result=threedi_result)

    #Convert netcdf to grid gpkg
    self.netcdf_to_grid_gpkg()

    #Replace waterlevel of selected cells with avg of neighbours.
    self.waterlevel_correction(output_col="wlvl_max_replaced")





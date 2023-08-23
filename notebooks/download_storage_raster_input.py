# %%
"""For storage raster creation we need """


from threedi_scenario_downloader import downloader as dl
dl.set_api_key('')



#Download rasters for area
uuids= {}
uuids['soil'] = '9e3534b7-b5d4-46ab-be35-4a0990379f76'
uuids['building'] = '98b5155d-dbc4-4a0c-a407-a9620741d308'

# Download rasters that are not on system yet.



dl.download_raster(scenario=uuids['soil'],
                            raster_code = "",
                            target_srs  = "EPSG:28992",
                            resolution  = meta.pixel_width,
                            bounds      = meta.bounds_dl,
                            bounds_srs  = "EPSG:28992",
                            pathname    = folder.input.soil.path,
                            is_threedi_scenario = False,
                            export_task_csv=folder.input.downloader_log.path)

# functions_bodemberging.download_lizard_rasters(uuid=uuids["soil"],
#                         output_path=folder.input.soil.path, 
#                         resolution=meta.pixel_width, 
#                         bounds=meta.bounds_dl)
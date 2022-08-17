import os
import re
from login import get_login_details

from threedi_scenario_downloader import downloader as dl

from batch_simulaties import get_model_and_simulation_name
from constants import *

lizard_username = get_login_details(option='username')
api_key = get_login_details(section='lizard', option='api_key')
srs = "EPSG:28992"
raster_resolution = 2


dl.set_api_key(api_key)


def download(scenario_name, out_dir, download_max_waterdepth=True, download_raw_results=False):
    print('Started ' + scenario_name)
    scenarios = dl.find_scenarios_by_name(scenario_name, limit=10)
    if len(scenarios) < 1:
        print(f'Scenario {scenario_name} not found')
        return
    for scen in scenarios:
        laundered_scen_name = re.sub("[^0-9a-zA-Z]+", "-", scen['name'])
        waterdepth_fn = laundered_scen_name + 'max_water_depth.tif'
        if download_max_waterdepth:
            print('Waiting for Lizard to generate max water depth tif...')
            dl.download_maximum_waterdepth_raster(scen['uuid'],
                                                  target_srs=srs,
                                                  resolution=raster_resolution,
                                                  pathname=os.path.join(out_dir, waterdepth_fn)
                                                  )
        if download_raw_results:
            print('Downloading raw results...')
            raw_results_dirname_full = os.path.join(out_dir, laundered_scen_name)
            try:
                os.makedirs(raw_results_dirname_full)
            except OSError as e:
                pass
            agg_url = dl.get_aggregation_netcdf_link((scen['uuid']))
            dl.download_file(agg_url, path=os.path.join(raw_results_dirname_full, 'aggregate_results_3di.nc'))
            dl.download_raw_results(scen['uuid'],
                                    pathname=os.path.join(raw_results_dirname_full, 'results_3di.nc'))
            dl.download_grid_administration(scen['uuid'],
                                            pathname=os.path.join(raw_results_dirname_full, 'gridadmin.h5'))
            log_url = dl.get_logging_link((scen['uuid']))
            dl.download_file(log_url, path=os.path.join(raw_results_dirname_full, 'log.zip'))
    dl.clear_inbox()


if __name__ == "__main__":
    for bui in ['T100', 'Radar']:
        for schematisation in SCHEMATISATIONS:
            model, scenario_name = get_model_and_simulation_name(schematisation_name=schematisation, bui=bui)
            out_dir = os.path.join(BASE_DIR, f"revision {model.revision_number}", "results", scenario_name)
            try:
                os.makedirs(out_dir)
            except OSError:
                pass

            download(
                scenario_name=scenario_name,
                out_dir=out_dir,
                download_max_waterdepth=False,
                download_raw_results=True
            )

#%%
import os
from asyncio.log import logger
import os
from threedi_api_client.openapi import ApiException
from threedi_api_client.api import ThreediApi
from threedi_api_client.versions import V3Api
from threedi_scenario_downloader import downloader as dl
import requests
import utils
from pathlib import Path


output_folder = r'E:\03.resultaten\Overstromingsberekeningen primaire doorbraken 2024\output'
# id_search = 'ROR-PRI-NOORDERDIJK VAN DRECHTERLAND (OOST)_'


#%%
def download_results_from_3di(output_folder, id_search, api_client):
    CHUNK_SIZE = 1024*1024 # 1MB
    
    # dl.set_api_key('Ssb7LXCk.sImsUmQLjXKHsNlaDs3tU0HHzPGQD8HN')    # SET YOUT OWN API KEY
    
    # # # Loggin code. 
    # API_KEY='5ba9BrZJ.yNxkRWDwFXH8w4U0ceyGt2RaYkjv8uWu'
    # config = {
    #     "THREEDI_API_HOST": "https://api.3di.live",
    #     "THREEDI_API_PERSONAL_API_TOKEN":API_KEY
    #     }
    # api_client: V3Api = ThreediApi(config=config, version='v3-beta')
    
    # #Loggin Confirmation Message
    # try:
    #     user = api_client.auth_profile_list()
    # except ApiException as e:
    #     print("Oops, something went wrong. Maybe you made a typo?")
    # else:
    #     print(f"Successfully logged in as {user.username}!")
    
    # # Create list of available scenarios
    # scenario1 = dl.find_scenarios_by_name(id_search, limit=1000)[0]
    
    # # Select the last results (0), and from it save the simulation id an the simulation name
    # simulation_id = dict(scenario1)['simulation_identifier']
    # simulation_name = dict(scenario1)['name']
    # id_search = 'ROR-PRI-BALGZANDDIJK_7_EN_BALGDIJK-T100000'
    # id_search = 'ROR-PRI-BALGZANDDIJK_3_EN_BALGDIJK-T100000'
    simulation = api_client.usage_list(simulation__name = id_search).results[0] 
    simulation_name = simulation.simulation.name
    simulation_id = simulation.simulation.id
    threedimodel_id = simulation.simulation.threedimodel_id
    # Download Gird
    response = api_client.threedimodels_gridadmin_download(threedimodel_id)
    grid_h5_path = os.path.join(output_folder,'gridadmin.h5')
    if response !=0:
        with requests.get(response.get_url, stream = True) as r:
            with open(grid_h5_path, 'wb') as f:
                print(f'downloading grid_adminstration for scenario {simulation_name}')
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    f.write(chunk)
    else:
        print('There is an error downloading the grid administration')
    
    
    # Download  the rest of the missing results
    logger.info("Downloading results into %s...", output_folder)
    simulation_results = api_client.simulations_results_files_list(simulation_id).results
    logger.debug("All simulation results: %s", simulation_results)

    # select files to download
    desired_results = [
        f"log_files_sim_{simulation_id}.zip",
        "results_3di.nc", 'aggregate_results_3di.nc',
    ]
    available_results = { simulation_result.filename.lower(): simulation_result
        for simulation_result in simulation_results
    }
    for desired_result in desired_results:
        if desired_result not in available_results:
            logger.error("Desired result file %s isn't available.", desired_result)
            continue

        resource = api_client.simulations_results_files_download(available_results[desired_result].id, simulation_id)
        target = Path(os.path.join(output_folder, desired_result))

        with requests.get(resource.get_url, stream=True) as r:
            with open(target, "wb") as f:
                print(f'downloading 3di file: {desired_result} for scenario {simulation_name}')
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    f.write(chunk)

        logger.info("Downloaded %s", target)
        expected_size = resource.size
        actual_size = target.stat().st_size
        if expected_size != actual_size:
            msg = (
                f"Incomplete download of {resource.get_url}: "
                f"expected {expected_size}, got {actual_size}."
            )
            raise utils.FileDownloadException(msg)
    print(f'scenario {simulation_name} downloaded')
    return simulation


# %%

# download_results_from_3di(output_folder, id_search)

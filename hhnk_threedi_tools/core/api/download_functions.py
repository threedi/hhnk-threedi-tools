import os, sys
import getpass  # Password
import requests  # API call
import json
import math
from tqdm import tqdm  # progressbar
import zipfile

"""Functions that interact with the results API"""


def retrieve_username_pw(username="", passw=""):
    """Get the headers"""
    if username == "":
        username = input("Enter username: ")
    if passw == "":
        passw = getpass.getpass("Enter password: ")
    headers = {"Content-Type": "application/json"}

    #     headers_results = {
    #     "username": '{}'.format(username),
    #     "password": '{}'.format(passw),
    #     "Content-Type": "application/json"
    #     }

    return headers, username, passw  # , headers_results


# def search_model_slug(base_url, headers_results, polder, revision_nr, custom_selection=0):
#     """With a given polder and revision, retrieve the model slug and scenario ID"""
#     result_limit = 1000

#     url = '{}scenarios/?name__icontains={}&model_revision={}&limit={}'.format(base_url, polder, revision_nr, result_limit)
#     try:
#         r = requests.get(url=url,headers=headers_results).json()
#         results = r['results']
#     except:
#         print('Login failed, check credentials')
#         results=[]
# #     if custom_selection == 1:
# #         for i, result in enumerate(results):
# #             print('{}: {}'.format(i+1,result['name']))
# #         scenario_ids = [int(x)-1 for x in input("Voer in welk scenario te downloaden (cijfers, kommagescheiden):").split(',') if x.strip().isdigit()]
# #     else:
# #         scenario_ids = [0]

#     return results


def create_download_url(results, scenario_ids, selected_results=""):
    """Based on the selected scenario (model run), create download link to the desired results"""

    download_raster = {}
    download_url = {}

    for scenario_id in scenario_ids:
        #         print("Processing scenario {}: {}".format(scenario_id+1,results[scenario_id]['name']))
        scenario = results[scenario_id]
        download_url[scenario["name"]] = []
        download_raster[scenario["name"]] = []

        # Determine which layers to download (automatically select the .h5 and .nc file if no further input is required)
        a = [key["result_type"]["name"] for key in scenario["result_set"]]
        if selected_results == "":
            result_ids = [
                a.index(b)
                for b in [
                    "raw 3Di output",
                    "grid administration",
                    "calculation core logging",
                    "aggregated 3Di output",
                ]
                if b in a
            ]
        else:
            # Get the id's of the selected result files (if they exist)
            result_ids = [a.index(b) for b in selected_results if b in a]
        
        for result_id in result_ids:
            if scenario["result_set"][result_id]["result_type"]["has_raster"]:
                download_raster[scenario["name"]].append(
                    scenario["result_set"][result_id]["raster"]["uuid"]
                )
            elif scenario["result_set"][result_id]["result_type"]["has_attachment"]:
                download_url[scenario["name"]].append(
                    scenario["result_set"][result_id]["attachment_url"]
                )
            else:
                print("Don't know what to do with the following object:")
                print(scenario["result_set"][result_id])
    #             print('')
    # print("Download url", download_url)
    return download_url


def start_download(download_url, output_folder, api_key, automatic_download):
    """Start downloading results with lizard API"""

    def download_file(download_url, output_folder):
        """Using the created links, check if the file is on the system and otherwise download them"""
        download_name = [
            a.rsplit("/")[-1] for a in download_url
        ]  # Get the name from the url
        for (index, url), name in zip(enumerate(download_url), download_name):
            download_path = os.path.join(output_folder, name)
            if not os.path.exists(download_path):
                # Start writing the file
                with open(download_path, "wb") as file:
                    print(str(index + 1) + ". Downloading to {}".format(download_path))
                    response = requests.get(url, auth=("__key__", api_key), stream=True)
                    response.raise_for_status()

                    total_length = int(response.headers.get("content-length"))

                    with tqdm(
                        total=math.ceil(total_length),
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                    ) as pbar:
                        for data in response.iter_content(1024 * 1024 * 10):
                            file.write(data)  # Schrijven verwerkte data.
                            file.flush()  # Interne buffer legen naar schijf. (belangrijk, anders geheugen probleem zonder stream!)
                            #                             os.fsync(file.fileno()) #Schrijf alles de uit buffer naar file op schijf zodat er geen gaten zijn.

                            pbar.set_postfix(
                                file=name, refresh=False
                            )  # Static text, showing filename.
                            pbar.update(len(data))  # Refresh the progressbar
                        pbar.close()

                # Unpack zipfile of log.
                if name == "log.zip":
                    zip_ref = zipfile.ZipFile(download_path, "r")
                    zip_ref.extractall(download_path.rstrip(".zip"))
                    zip_ref.close()

            else:
                print("{}. File {} is already on the system".format(index + 1, name))

    # Downloading of these files
    print("\n\033[1mStarting the download \033[0m")
    if automatic_download == 0:
        proceed_download = input("Proceed to download? [y/n]: ")
    else:
        proceed_download = "y"
    if proceed_download == "y":
        download_file(download_url, output_folder)
        print("All file downloads finished!")
    else:
        print("Process aborted.")

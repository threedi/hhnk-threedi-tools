import os, sys
import requests  # API call
import math
from tqdm import tqdm  # progressbar
import zipfile

"""Functions that interact with the results API"""


def start_download(download_url, output_folder, api_key, automatic_download):
    """Start downloading results with lizard API"""

    def download_file(download_url, output_folder):
        """Using the created links, check if the file is on the system and otherwise download them"""
        download_name = [a.rsplit("/")[-1] for a in download_url]  # Get the name from the url

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

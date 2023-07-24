# -*- coding: utf-8 -*-

"""2023-07-24 threedi_scenario_downloader v1.2 works properly with v4 API. This isnt needed anymore."""
"""The downloader part of the threedi_scenario_downloader supplies the user with often used functionality to look up and export 3Di results using the Lizard API"""
from datetime import datetime
from urllib.parse import urlparse
from urllib.error import HTTPError
from time import sleep
import logging
import os
import requests
import csv
import pyproj
import shapely

"""
2023-02-06 Wietse; V4 downloaden gaat niet lekker, maar zoeken is wel sneller. Dus het is nu half half geimplementeerd. 

Dit is een bewerkte kopie op threedi_scenario_downloader
"""

LIZARD_URL = "https://demo.lizard.net/api/v3/"
LIZARD_URL_V4 = "https://demo.lizard.net/api/v4/"
RESULT_LIMIT = 10

log = logging.getLogger()
AUTH = {}

SCENARIO_FILTERS = {
    "name": "name",
    "name__icontains": "name__icontains",
    "uuid": "uuid",
    "id": "id",
    "model_revision": "model_revision",
    "model_name": "model_name__icontains",
    "organisation": "organisation__icontains",
    "organisation__unique_id": "organisation__unique_id",
    "username": "username__icontains",
    "offset": "offset",
}


def set_logging_level(level):
    """set logging level to the supplied level"""

    log.level = level


def set_api_key(api_key):
    AUTH["api_key"] = api_key


def get_api_key():
    return AUTH["api_key"]


def find_scenarios(limit=RESULT_LIMIT, **kwargs):
    """return json containing scenarios based on supplied filters"""
    url = "{}scenarios/".format(LIZARD_URL_V4)

    payload = {"limit": limit}
    for key, value in kwargs.items():
        api_filter = SCENARIO_FILTERS[key]
        payload[api_filter] = value

    r = requests.get(url=url, auth=("__key__", get_api_key()), params=payload)
    r.raise_for_status()
    return r.json()["results"]


def find_scenarios_by_model_slug(model_uuid, limit=RESULT_LIMIT):
    """return json containing scenarios based on model slug"""

    url = "{}scenarios/".format(LIZARD_URL)
    payload = {"model_name__icontains": model_uuid, "limit": limit}
    r = requests.get(url=url, auth=("__key__", get_api_key()), params=payload)
    r.raise_for_status()
    return r.json()["results"]


def find_scenarios_by_name(name, limit=RESULT_LIMIT):
    """return json containing scenarios based on name"""
    url = "{}scenarios/".format(LIZARD_URL)
    payload = {"name__icontains": name, "limit": limit}
    r = requests.get(url=url, auth=("__key__", get_api_key()), params=payload)
    r.raise_for_status()
    return r.json()["results"]


def find_scenarios_by_exact_name(name, limit=RESULT_LIMIT):
    """return json containing scenarios based on exact name"""
    url = "{}scenarios/".format(LIZARD_URL)
    payload = {"name": name, "limit": limit}
    r = requests.get(url=url, auth=("__key__", get_api_key()), params=payload)
    r.raise_for_status()
    return r.json()["results"]


def get_netcdf_link(scenario_uuid):
    """return url to raw 3Di results"""
    r = requests.get(
        url="{}scenarios/{}".format(LIZARD_URL, scenario_uuid),
        auth=("__key__", get_api_key()),
    )
    r.raise_for_status()
    for result in r.json()["result_set"]:
        if result["result_type"]["code"] == "results-3di":
            url = result["attachment_url"]
            return url


def get_aggregation_netcdf_link(scenario_uuid):
    """return url to raw 3Di results"""
    r = requests.get(
        url="{}scenarios/{}".format(LIZARD_URL, scenario_uuid),
        auth=("__key__", get_api_key()),
    )
    r.raise_for_status()
    for result in r.json()["result_set"]:
        if result["result_type"]["code"] == "aggregate-results-3di":
            url = result["attachment_url"]
            return url


def get_gridadmin_link(scenario_uuid):
    """return url to gridadministration"""
    r = requests.get(
        url="{}scenarios/{}".format(LIZARD_URL, scenario_uuid),
        auth=("__key__", get_api_key()),
    )
    r.raise_for_status()
    for result in r.json()["result_set"]:
        if result["result_type"]["code"] == "grid-admin":
            url = result["attachment_url"]
            return url


def get_logging_link(scenario_uuid):
    """return url to zipped logging"""
    r = requests.get(
        url="{}scenarios/{}".format(LIZARD_URL, scenario_uuid),
        auth=("__key__", get_api_key()),
    )
    r.raise_for_status()
    for result in r.json()["result_set"]:
        if result["result_type"]["code"] == "logfiles":
            url = result["attachment_url"]
            return url


def get_raster(scenario_uuid, raster_code):
    """return json of raster based on scenario uuid and raster type"""

    if "v3" in LIZARD_URL:
        r = requests.get(
            url="{}scenarios/{}".format(LIZARD_URL, scenario_uuid),
            auth=("__key__", get_api_key()),
        )
        r.raise_for_status()
        for result in r.json()["result_set"]:
            if result["result_type"]["code"] == raster_code:
                return result["raster"]

    if "v4" in LIZARD_URL:
        r = requests.get(
            url="{}scenarios/{}/results".format(LIZARD_URL, scenario_uuid),
            auth=("__key__", get_api_key()),
            params={"limit": 20}, #Get all results.. not just first 10
        )
        r.raise_for_status()

        for result in r.json()["results"]:
            if result["code"] == raster_code:
                r2 = requests.get(
                    url=result["raster"],
                    auth=("__key__", get_api_key())
                )
                return r2.json()
                
    


def create_raster_task(
    raster, target_srs, resolution, bounds=None, bounds_srs=None, time=None
):
    """create Lizard raster task"""

    if bounds == None:
        bounds = raster["spatial_bounds"]
        bounds_srs = "EPSG:4326"

    # if target_srs != bounds_srs:
    #     [w,e],[s,n] = pyproj.transform("EPSG:4326", "EPSG:28992", y=[bounds['west'], bounds['east']], x=[bounds['south'], bounds['north']])
    #     print("boundstransform")
    # else:
    e = bounds["east"]
    w = bounds["west"]
    n = bounds["north"]
    s = bounds["south"]


    if "v3" in LIZARD_URL:
        bounds_name = "geom"
        bounds_srs_name = "srs"
        bbox = str(shapely.geometry.box(w,s,e,n))

    if "v4" in LIZARD_URL:
        bounds_name = "bbox"
        bounds_srs_name = "projection"
        bbox = f"{w},{s},{e},{n}"
        raise Exception(NotImplementedError("V4 handles resolution differently, it requires width and height instead of cellsize"))

    url = "{}rasters/{}/data/".format(LIZARD_URL, raster["uuid"])
    if time is None:
        # non temporal raster
        payload = {
            "cellsize": resolution,
            bounds_name: bbox,
            bounds_srs_name: bounds_srs,
            "target_srs": target_srs,
            "format": "geotiff",
            "async": "true",
        }
    else:
        # temporal rasters
        payload = {
            "cellsize": resolution,
            bounds_name: bbox,
            bounds_srs_name: bounds_srs,
            "target_srs": target_srs,
            "time": time,
            "format": "geotiff",
            "async": "true",
        }
        print(url)
    r = requests.get(url=url, auth=("__key__", get_api_key()), params=payload)
    r.raise_for_status()
    return r.json()


# From here untested methods are added
def get_task_status(task_uuid):
    """return status of task"""
    url = "{}tasks/{}/".format(LIZARD_URL, task_uuid)
    try:
        r = requests.get(url=url, auth=("__key__", get_api_key()))
        r.raise_for_status()
        if "v3" in LIZARD_URL:
            return r.json()["task_status"]
        if "v4" in LIZARD_URL:
            return r.json()["status"]

    except:
        return "UNKNOWN"


def get_task_download_url(task_uuid):
    """return url of successful task"""
    if get_task_status(task_uuid) == "SUCCESS":
        url = "{}tasks/{}/".format(LIZARD_URL, task_uuid)
        r = requests.get(url=url, auth=("__key__", get_api_key()))
        r.raise_for_status()
        if "v3" in LIZARD_URL:
            return r.json()["result_url"]
        if "v4" in LIZARD_URL:
            return r.json()["result"]
    # What to do if task is not a success?


def download_file(url, path):
    """download url to specified path"""
    logging.debug("Start downloading file: {}".format(url))
    r = requests.get(url, auth=("__key__", get_api_key()), stream=True)
    r.raise_for_status()
    with open(path, "wb") as file:
        for chunk in r.iter_content(1024 * 1024 * 10):
            file.write(chunk)


def download_task(task_uuid, pathname=None):
    """download result of successful task"""
    if get_task_status(task_uuid) == "SUCCESS":
        download_url = get_task_download_url(task_uuid)
        if pathname is None:

            logging.debug("download_url: {}".format(download_url))
            logging.debug("urlparse(download_url): {}".format(urlparse(download_url)))
            pathname = os.path.basename(urlparse(download_url).path)
            logging.debug(pathname)
        download_file(download_url, pathname)


def download_raster(
    scenario,
    raster_code=None,
    target_srs=None,
    resolution=None,
    bounds=None,
    bounds_srs=None,
    time=None,
    pathname=None,
    is_threedi_scenario=True,  # For lizard rasters that are not a Threedi result.
    export_task_csv=None,
):
    """
    Download raster.
    To download multiple rasters at the same time, simply pass the required input parameters as list.
    Scenario and pathname should be of same length. Other paramerts can be tuple to apply the same settings to all rasters.
    """
    # If task is called for single raster, prepare list.
    def transform_to_list(var, length=1):
        """Transform input to list if for instance only one input is given"""
        if type(var) is list:
            return var
        else:
            if type(var) is tuple:
                return list(var) * length
            else:  # type(var) in (str, dict, int, type(None), bool, float):
                return [var] * length

    # Transform input parameters to list
    scenario_list = transform_to_list(var=scenario)
    raster_code_list = transform_to_list(var=raster_code, length=len(scenario_list))
    target_srs_list = transform_to_list(var=target_srs, length=len(scenario_list))

    bounds_list = transform_to_list(var=bounds, length=len(scenario_list))
    bounds_srs_list = transform_to_list(var=bounds_srs, length=len(scenario_list))
    resolution_list = transform_to_list(var=resolution, length=len(scenario_list))
    time_list = transform_to_list(var=time, length=len(scenario_list))
    pathname_list = transform_to_list(var=pathname)
    is_threedi_scenario_list = transform_to_list(
        var=is_threedi_scenario, length=len(scenario_list)
    )

    # Helper parameters.
    processed_list = transform_to_list(var=False, length=len(scenario_list))
    task_id_list = transform_to_list(var=None, length=len(scenario_list))
    task_url_list = transform_to_list(var=None, length=len(scenario_list))

    # Wrong input error
    if len(scenario_list) != len(pathname_list):
        logging.debug("Scenarios and output should be of same length")
        raise ValueError("scenario_list and pathname_list are of different length")

    tasks = []
    # Create tasks
    for (
        (index, scenario),
        raster_code,
        target_srs,
        bounds,
        bounds_srs,
        resolution,
        time,
        is_threedi_scenario,
    ) in zip(
        enumerate(scenario_list),
        raster_code_list,
        target_srs_list,
        bounds_list,
        bounds_srs_list,
        resolution_list,
        time_list,
        is_threedi_scenario_list,
    ):
        if is_threedi_scenario:
            if type(scenario) is str:
                # assume uuid
                raster = get_raster(scenario, raster_code)
            elif type(scenario) is dict:
                # assume json object
                raster = get_raster_from_json(scenario, raster_code)
            else:
                logging.debug("Invalid scenario: supply a json object or uuid string")
                raise ValueError(
                    "Invalid scenario: supply a json object or uuid string"
                )
        else:
            # If no bounds are passed the function will probably crash.
            if (type(scenario) is str) and (bounds is not None):
                raster = {}
                raster["uuid"] = scenario
            else:
                # print("Invalid scenario: supply a uuid string and spatial bounds. Scenario: {}".format(scenario))
                logging.debug(
                    "Invalid scenario: supply a uuid string and spatial bounds"
                )
        # Send task to lizard
        logging.debug("Creating task with the following parameters:")
        logging.debug("raster: {}".format(raster))
        logging.debug("target_srs: {}".format(target_srs))
        logging.debug("resolution: {}".format(resolution))
        logging.debug("bounds: {}".format(bounds))
        logging.debug("bounds_srs: {}".format(bounds_srs))
        logging.debug("time: {}".format(time))

        task = create_raster_task(
            raster,
            target_srs,
            resolution=resolution,
            bounds=bounds,
            bounds_srs=bounds_srs,
            time=time,
        )
        task_id_list[index] = task["task_id"]
        task_url_list[index] = task["url"]
        tasks.append(task)

    if export_task_csv is not None:
        logging.debug("Exporting tasks to csv")

        task_export = []

        # Create a list with task url's and pathnames
        for (index, task_id), task_url, pathname in zip(
            enumerate(task_id_list), task_url_list, pathname_list
        ):
            task_export.append({"uuid": task_id, "url": task_url, "pathname": pathname})

        logging.debug("task_export: {}".format(task_export))
        with open(export_task_csv, "w", newline="") as f:

            # using csv.writer method from CSV package
            field_names = ["uuid", "url", "pathname"]
            writer = csv.DictWriter(
                f, field_names, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL
            )
            writer.writeheader()
            writer.writerows(task_export)

    # Check status of task and download
    while not all(processed_list):
        for (index, task_uuid), pathname, processed in zip(
            enumerate(task_id_list), pathname_list, processed_list
        ):
            if not processed:

                task_status = get_task_status(task_uuid)

                if task_status == "SUCCESS":
                    # task is a succes, return download url
                    try:
                        logging.debug(
                            "Task succeeded, start downloading url: {}".format(
                                get_task_download_url(task_uuid)
                            )
                        )
                        logging.debug(
                            "Remaining tasks: {}".format(
                                processed_list.count(False) - 1
                            )
                        )
                        download_task(task_uuid, pathname)
                        processed_list[index] = True

                    except HTTPError as err:
                        if err.code == 503:
                            logging.debug(
                                "503 Server Error: Lizard has lost it. Let's ignore this."
                            )
                            task_status = "UNKNOWN"
                        else:
                            raise

                elif task_status in ("PENDING", "UNKNOWN", "STARTED", "RETRY"):
                    pass
                else:
                    print("Download raster task failed")
                    logging.debug(
                        "Task {} failed, status was: {}".format(task_uuid, task_status)
                    )
                    processed_list[index] = True
        sleep(5)


def download_maximum_waterdepth_raster(
    scenario_uuid, target_srs, resolution, bounds=None, bounds_srs=None, pathname=None
):
    """download Maximum waterdepth raster"""
    download_raster(
        scenario_uuid,
        "depth-max-dtri",
        target_srs,
        resolution,
        bounds=bounds,
        bounds_srs=bounds_srs,
        pathname=pathname,
    )


def download_maximum_waterlevel_raster(
    scenario_uuid, target_srs, resolution, bounds=None, bounds_srs=None, pathname=None
):
    """download Maximum waterdepth raster"""
    download_raster(
        scenario_uuid,
        "s1-max-dtri",
        target_srs,
        resolution,
        bounds=bounds,
        bounds_srs=bounds_srs,
        pathname=pathname,
    )


def download_total_damage_raster(
    scenario_uuid, target_srs, resolution, bounds=None, bounds_srs=None, pathname=None
):
    """download Total Damage raster"""
    download_raster(
        scenario_uuid,
        "total-damage",
        target_srs,
        resolution,
        bounds=bounds,
        bounds_srs=bounds_srs,
        pathname=pathname,
    )


def download_waterdepth_raster(
    scenario_uuid,
    target_srs,
    resolution,
    time,
    bounds=None,
    bounds_srs=None,
    pathname=None,
):
    """download snapshot of Waterdepth raster"""
    download_raster(
        scenario_uuid,
        "depth-dtri",
        target_srs,
        resolution,
        bounds=bounds,
        bounds_srs=bounds_srs,
        time=time,
        pathname=pathname,
    )


def download_waterlevel_raster(
    scenario_uuid,
    target_srs,
    resolution,
    time,
    bounds=None,
    bounds_srs=None,
    pathname=None,
):
    """download snapshot of Waterdepth raster"""
    download_raster(
        scenario_uuid,
        "s1-dtri",
        target_srs,
        resolution,
        bounds=bounds,
        bounds_srs=bounds_srs,
        time=time,
        pathname=pathname,
    )


def download_precipitation_raster(
    scenario_uuid,
    target_srs,
    resolution,
    time,
    bounds=None,
    bounds_srs=None,
    pathname=None,
):
    """download snapshot of Waterdepth raster"""
    download_raster(
        scenario_uuid,
        "rain-quad",
        target_srs,
        resolution,
        bounds=bounds,
        bounds_srs=bounds_srs,
        time=time,
        pathname=pathname,
    )


def download_raw_results(scenario_uuid, pathname=None):
    """downloads the 3Di NetCDF file of the supplied scenario"""
    url = get_netcdf_link(scenario_uuid)
    logging.debug("Start downloading raw results: {}".format(url))
    download_file(url, pathname)


def download_aggregated_results(scenario_uuid, pathname=None):
    """downloads the 3Di aggregated NetCDF file of the supplied scenario"""
    url = get_aggregation_netcdf_link(scenario_uuid)
    logging.debug("Start downloading aggregated results: {}".format(url))
    download_file(url, pathname)


def download_logging_results(scenario_uuid, pathname=None):
    """downloads the 3Di logging of the supplied scenario"""
    url = get_logging_link(scenario_uuid)
    logging.debug("Start downloading logging results: {}".format(url))
    download_file(url, pathname)


def download_grid_administration(scenario_uuid, pathname=None):
    """downloads the 3Di grid administration (.h5 file) of the supplied scenario"""
    url = get_gridadmin_link(scenario_uuid)
    logging.debug("Start downloading grid administration: {}".format(url))
    download_file(url, pathname)


def clear_inbox():
    """delete all messages from Lizard inbox"""
    url = "{}inbox/".format(LIZARD_URL)
    r = requests.get(
        url=url,
        auth=("__key__", get_api_key()),
        params={"limit": RESULT_LIMIT},
        timeout=10,
    )
    r.raise_for_status()
    messages = r.json()["results"]
    for msg in messages:
        msg_id = msg["id"]
        read_url = "{}inbox/{}/read/".format(LIZARD_URL, msg_id)
        r = requests.post(url=read_url, auth=("__key__", get_api_key()), timeout=10)
    return True


def get_attachment_links(scenario_json):
    """get links to static scenario results"""
    attachment_links = {}
    for result in scenario_json["result_set"]:
        if result["attachment_url"]:
            result_name = result["result_type"]["name"]
            attachment_links[result_name] = result["attachment_url"]
    if attachment_links:
        return attachment_links
    else:
        return None


def rasters_in_scenario(scenario_json):
    """return two lists of static and temporal rasters including 3di result name and code"""
    temporal_rasters = []
    static_rasters = []
    for result in scenario_json["result_set"]:
        result_type = result["result_type"]
        if result_type["has_raster"]:
            raster = result["raster"]
            name_3di = result_type["name"]
            code_3di = result_type["code"]
            raster["name_3di"] = name_3di
            raster["code_3di"] = code_3di
            if raster["temporal"]:
                temporal_rasters.append(raster)
            else:
                static_rasters.append(raster)
    return static_rasters, temporal_rasters


def get_raster_link(
    raster, target_srs, resolution, bounds=None, bounds_srs=None, time=None
):
    """get url to download raster"""
    task = create_raster_task(raster, target_srs, resolution, bounds, bounds_srs, time)
    task_uuid = task["task_id"]

    logging.debug("Start waiting for task {} to finish".format(task_uuid))
    task_status = get_task_status(task_uuid)
    while task_status == "PENDING":
        logging.debug("Still waiting for task {}".format(task_uuid))
        sleep(5)
        task_status = get_task_status(task_uuid)

    if get_task_status(task_uuid) == "SUCCESS":
        # task is a succes, return download url
        download_url = get_task_download_url(task_uuid)
        return download_url
    else:
        logging.debug("Task failed")
        return None


def get_static_rasters_links(
    static_rasters, target_srs, resolution, bounds=None, bounds_srs=None, time=None
):
    """return a dict of urls to geotiff files of static rasters in scenario
    the dict items are formatted as result_name: link.tif"""
    static_raster_urls = {}
    for static_raster in static_rasters:
        name = static_raster["name_3di"]
        static_raster_url = get_raster_link(
            static_raster, target_srs, resolution, bounds, bounds_srs, time
        )
        static_raster_urls[name] = static_raster_url
    return static_raster_urls


def get_temporal_raster_links(
    temporal_raster,
    target_srs,
    resolution,
    bounds=None,
    bounds_srs=None,
    interval_hours=None,
):
    """return a dict of urls to geotiff files of a temporal raster
    the dict items are formatted as name_3di_datetime: link.tif"""
    temporal_raster_urls = {}
    name = temporal_raster["name_3di"]
    timesteps = get_raster_timesteps(temporal_raster, interval_hours)
    for timestep in timesteps:
        download_url = get_raster_link(
            temporal_raster, target_srs, resolution, bounds, timestep
        )
        url_timestep = os.path.splitext(download_url)[0].split("_")[-1]
        # Lizard returns the nearest timestep based on the time=timestep request
        timestep_url_format = "{}Z".format(timestep.split(".")[0].replace("-", ""))
        if timestep_url_format == url_timestep:
            # when requested and retrieved timesteps are equal, use the timestep
            name_timestep = "_".join([name, timestep])
        else:
            # if not equal, indicate the datetime discrepancy in file name
            name_timestep = "{}_get_{}_got_{}".format(
                name, timestep_url_format, url_timestep
            )
        temporal_raster_urls[name_timestep] = download_url
    return temporal_raster_urls


def get_temporal_rasters_links(
    temporal_rasters,
    target_srs,
    resolution,
    bounds=None,
    bounds_srs=None,
    interval_hours=None,
):
    """get links to all temporal rasters"""
    temporal_rasters_urls = {}
    for temporal_raster in temporal_rasters:
        temporal_raster_urls = get_temporal_raster_links(
            temporal_raster, target_srs, resolution, bounds, bounds_srs, interval_hours
        )
        for name_timestep, download_url in temporal_raster_urls.items():
            temporal_rasters_urls.setdefault(name_timestep, download_url)
    return temporal_rasters_urls


def get_raster_timesteps(raster, interval_hours=None):
    """returns a list of 'YYYY-MM-DDTHH:MM:SS' formatted timesteps in temporal range of raster object
    Starts at first timestep and ends at last timestep.
    The intermediate timesteps are determined by the interval.
    When no interval is provided, the first, middle and last timesteps are returned
    """
    raster_uuid = raster["uuid"]
    if not raster["temporal"]:
        return [None]
    if not interval_hours:
        # assume interval of store (rounded minutes) and return first, middle and last raster
        url = "{}rasters/{}/timesteps/".format(LIZARD_URL, raster_uuid)
        timesteps_json = request_json_from_url(url)
        timesteps_ms = timesteps_json["steps"]
        # only return first, middle and last raster
        timesteps_ms = [
            timesteps_ms[0],
            timesteps_ms[round(len(timesteps_ms) / 2)],
            timesteps_ms[-1],
        ]
    else:
        # use interval from argument
        first_timestamp = int(raster["first_value_timestamp"])
        timesteps_ms = []
        last_timestamp = int(raster["last_value_timestamp"])
        interval_ms = interval_hours * 3600000
        while last_timestamp > first_timestamp:
            timesteps_ms.append(first_timestamp)
            first_timestamp += interval_ms
        if not last_timestamp in timesteps_ms:
            timesteps_ms.append(last_timestamp)
    timesteps = [datetime.fromtimestamp(i / 1000.0).isoformat() for i in timesteps_ms]
    return timesteps


def get_raster_from_json(scenario, raster_code):
    """return raster json object from scenario"""
    for result in scenario["result_set"]:
        if result["result_type"]["code"] == raster_code:
            return result["raster"]


def request_json_from_url(url, params=None):
    """retrieve json object from url"""
    r = requests.get(url=url, auth=("__key__", get_api_key()), params=params)
    r.raise_for_status()
    if r.status_code == requests.codes.ok:
        return r.json()


def resume_download_tasks(task_file, overwrite=False):
    """read csv with tasks and resume downloading the succesfull tasks"""

    processed_tasks = []
    unprocessed_tasks = []

    # Read tasks from file
    with open(task_file, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            unprocessed_tasks.append(row)
            task_url = row["url"]
            logging.debug("Reading task file, line: {}".format(task_url))

    while len(unprocessed_tasks) > 0:
        for task in unprocessed_tasks:
            uuid = task["uuid"]
            pathname = task["pathname"]

            task_status = get_task_status(uuid)

            if task_status == "SUCCESS":
                # Task succesfull, check if file already exists

                # Download if it doesn't exist, or if it do
                if not os.path.isfile(pathname) or overwrite:
                    try:
                        download_task(uuid, pathname)
                    except HTTPError as err:
                        if err.code == 503:
                            logging.debug(
                                "503 Server Error: Lizard has lost it. Let's ignore this."
                            )
                            task_status = "UNKNOWN"
                        else:
                            raise

                # move task to processed list
                processed_tasks.append(task)
                unprocessed_tasks.remove(task)

            elif task_status in ("PENDING", "UNKNOWN", "STARTED", "RETRY"):
                pass
            else:
                logging.debug(
                    "Task {} failed, status was: {}".format(uuid, task_status)
                )
        sleep(5)

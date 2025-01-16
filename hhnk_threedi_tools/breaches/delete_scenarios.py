# %%
import requests
import pandas as pd
csv_location = pd.read_excel(
    r"E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\ldo_structuur\delete_id.xlsx"
)


# json_file  = r"E:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\ldo_structuur\response_1735297760045.json"
def extract_ids(json_data):
    ids = []
    for item in json_data["items"]:
        if "id" in item:
            ids.append(item["id"])
    return ids


# %%
# Test API connection
# Copy the hearders from the swagger website.
health = "https://www.overstromingsinformatie.nl/auth/health/"
headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "X-CSRFToken": "29TVZT8M1YqAYO8xtU4jqeGYSA6zijTNarMpXLanXDacq0lGcrUsEESrEL17E79r",
}
response_health = requests.get(url=health, headers=headers)
print(response_health.json())

# %%
well_know = "https://www.overstromingsinformatie.nl/auth/.well-known/jwks.json"
well_know_response = requests.get(url=well_know, headers=headers)
print(well_know_response.json())
# %%
# FOR ADMINISTRATION PERMISION USE THE FOLLOWING. Otherwise you will get a permission feedback
# at the moment you will try to upload the excel file.

parameters = {
    "scope": "admin",
    "name": "Juan_Test_12",
    "expiry_date": "2024-12-13T06:54:04.597Z",
    "revoked": False,
}


# Copy here the API key generated on the website

api_key_10_07_24 = "lask2hq6.JhTTsbYLI0j5FNF20JQNpubBaYpByIx0"
# %%
# Check Tenants
tenants = "https://www.overstromingsinformatie.nl/auth/v1/tenants/"
response_tenants = requests.get(
    url=tenants, headers=headers, auth=("__key__", api_key_10_07_24)
)
print(response_tenants.json())

# %%
# Get Token
token_url = "https://www.overstromingsinformatie.nl/auth/v1/token/"
response_5 = requests.post(
    url=token_url, json={"tenant": 4}, auth=("__key__", api_key_10_07_24)
)
print(response_5.json())
refresh = response_5.json()["refresh"]

# Get the TokenRefresh
access = response_5.json()
refresh_url = "https://www.overstromingsinformatie.nl/auth/v1/token/refresh/"
data_refresh = {"refresh": response_5.json()["refresh"]}
response_refresh = requests.post(
    url=refresh_url, json=data_refresh, auth=("__key__", api_key_10_07_24)
)
response_refresh = response_refresh.json()
refresh_token = response_refresh["access"]
print(response_refresh)


# Get ids to delete
file_import_url = "https://www.overstromingsinformatie.nl/api/v1/scenarios?mode=private&limit=100&offset=0&order_by=id&status=incomplete"
headers_excel = {
    "accept": "application/json",
    "authorization": f"Bearer {refresh_token}",
    # 'Content-type':'application/zip',
}
response_incomplete = requests.get(url=file_import_url, headers=headers_excel)

# Get json file from the link to be extracted
# %%
# id_scenarios = extract_ids(response_incomplete.json())
id_scenarios = csv_location['id_delete'].values

# %%
# id_scenarios = []
for id_scenario in id_scenarios:
    file_import_url = (
        f"https://www.overstromingsinformatie.nl/api/v1/scenarios/{id_scenario}"
    )
    # file_import_url = f'https://ldo.staging.lizard.net/api/v1/excel-imports/{id_excel}/files/{zip_name}/upload'
    headers_excel = {
        "accept": "application/json",
        "authorization": f"Bearer {refresh_token}",
        # 'Content-type':'application/zip',
    }

    response = requests.delete(url=file_import_url, headers=headers_excel)
    print(file_import_url)
# %%


# %%

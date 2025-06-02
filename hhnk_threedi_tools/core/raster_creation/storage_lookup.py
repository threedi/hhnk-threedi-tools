# %%
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd

import hhnk_threedi_tools as htt


def create_storage_lookup(rootzone_thickness_cm, storage_unsa_sim_path=None) -> pd.DataFrame:
    """Create a list of available storage for given rootzone thickness

    The unsa_sim.inp is a table that contains storage per soil type for different dewathering depths and depths of the root zone. It gives the amount of storage available in the unsaturated zone, which is a non-linear relation to the dewathering depth.
    See https://content.oss.deltares.nl/delft3d/manuals/SOBEK_User_Manual.pdf p817
    Assumption 1: Sand is used for raising project areas, which is soil type 14: Podzolic, coarse textured sandy soil: sandy soil (gHd30).
    Assumption 2: Project areas left unpaved are mainly grown with grass, which according to ROOT_SIM.INP translates to a root zone thickness of 0.2 m.
    Example: UNSA_SIM.INP
    column 1 = soil type
    column 2 = rootzone thickness (cm)
    column 3 = groundwater level (meter below surface)
    column 4 = root zone soil moisture storage in equilibrium conditions (mm)
    column 5 = potential capillary rise (mm/day)
    column 6 = storage coefficient (m/m)
    Column 6 gives the local storage in the unsaturated zone, not the total! So this value must be integrated over the entire depth of the soil. Also, the rooth zone is a seperate reservoir. The difference in storage in the local and maximum rooth zone (column 4) must be added to the storage in the unsaturated zone to find the total storage.
    Storage at depth d
    =
    Integrated over d of storage coefficient
    +
    storage in the rooth zone at depth = 0
    -
    storage at the roost zone at depth d

    """

    if storage_unsa_sim_path is None:
        storage_unsa_sim_path = hrt.get_pkg_resource_path(package_resource=htt.resources, name="unsa_sim.csv")

    storage_df = pd.read_csv(str(storage_unsa_sim_path), sep=";")
    storage_df.rename(
        {
            "soil type": "soil_type",
            "rootzone thickness (cm)": "rootzone_thickness",
            "groundwater level (meter below surface)": "dewatering_depth",
            "storage coefficient (m/m)": "storage_coefficient",
        },
        axis=1,
        inplace=True,
    )

    storage_df = storage_df[storage_df["rootzone_thickness"] == rootzone_thickness_cm]

    # Compute total available storage at all depths
    dstep = 0.01
    depths = np.arange(0, 10 + dstep, dstep)

    storage_lookup_dict = []
    local_storage_sum = 0

    for soil_type in np.unique(storage_df["soil_type"]):
        # Create list of dewateringdepths and corresponding storage coefficient and roothzone storage from capsim table.
        # ontwateringsdiepte
        xlist = np.round(storage_df.loc[storage_df["soil_type"] == soil_type, "dewatering_depth"].tolist(), 5)
        # bergingscoefficient
        sclist = np.round(storage_df.loc[storage_df["soil_type"] == soil_type, "storage_coefficient"].tolist(), 5)
        rzlist = np.round(
            storage_df.loc[
                storage_df["soil_type"] == soil_type, "root zone soil moisture storage in equilibrium conditions (mm)"
            ].tolist(),
            5,
        )  # y = bergingscoefficient

        max_soil_rooth_storage = rzlist.max()

        for d in depths:
            # calculate and summate storage in unsaturated zone

            if d == 0:
                local_coef = 0
                local_storage = 0
                local_storage_sum = 0
            else:
                local_coef = np.interp(x=d, xp=xlist, fp=sclist)
                local_storage = local_coef * dstep * 1000  # converted to mm as rooth zone storage is too
                local_storage_sum = local_storage_sum + local_storage

            # calculate storage in rooth zone
            rooth_storage = np.interp(x=d, xp=xlist, fp=rzlist)
            avail_rooth_storage = max_soil_rooth_storage - rooth_storage

            # Total availeble storage
            total_storage = (avail_rooth_storage + local_storage_sum) / 1000  # back to meters

            storage_lookup_dict.append(
                {
                    "Soil Type": soil_type,
                    "Rootzone Thickness (cm)": rootzone_thickness_cm,
                    "Dewathering Depth (m)": d,
                    "Storage Coefficient (m/m)": local_coef,
                    "Local Storage (mm)": local_storage,
                    "Unsa Zone Storage (mm)": local_storage_sum,
                    "Local Total Rooth Zone Storage (mm)": rooth_storage,
                    "Available Rooth Zone Storage (mm)": avail_rooth_storage,
                    "Total Available Storage (m)": total_storage,
                }
            )

    storage_lookup_df = pd.DataFrame(storage_lookup_dict)
    soil_lookup_df = storage_lookup_df.groupby("Soil Type").agg(
        {
            "Dewathering Depth (m)": list,
            "Total Available Storage (m)": lambda x: list(round(x, 5)),
        }
    )

    return storage_lookup_df, soil_lookup_df

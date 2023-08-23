# %%
import numpy as np
import pandas as pd
import os


def create_storage_lookup(storage_unsa_sim_path, rootzone_thickness) -> pd.DataFrame:
    """function creates a list of available storage for given rootzone thickness"""

    storage_df = pd.read_csv(str(storage_unsa_sim_path), sep=';')
    storage_df.rename({'soil type':'soil_type',
                    'rootzone thickness (cm)': 'rootzone_thickness',
                    'groundwater level (meter below surface)': 'dewatering_depth',
                    'storage coefficient (m/m)': 'storage_coefficient',
                    }, axis=1, inplace=True)

    storage_df = storage_df[storage_df['rootzone_thickness']==rootzone_thickness]

    # Compute total available storage at all depths
    dstep = 0.01
    depths = np.arange(0,10+dstep,dstep)

    storage_lookup_dict = []
    local_storage_sum = 0

    for soil_type in np.unique(storage_df['soil_type']):

        #Create list of dewateringdepths and corresponding storage coefficient and roothzone storage from capsim table.
        #ontwateringsdiepte 
        xlist = np.round(storage_df.loc[storage_df['soil_type']==soil_type, 
                                        'dewatering_depth'].tolist(),5)
        #bergingscoefficient
        sclist = np.round(storage_df.loc[storage_df['soil_type']==soil_type, 
                                         'storage_coefficient'].tolist(),5)
        rzlist = np.round(storage_df.loc[storage_df['soil_type']==soil_type, 
                                         'root zone soil moisture storage in equilibrium conditions (mm)'].tolist(),5) # y = bergingscoefficient

        max_soil_rooth_storage = rzlist.max()

        for d in depths:
            # calculate and summate storage in unsaturated zone

            if d == 0:
                local_coef=0
                local_storage = 0
                local_storage_sum = 0
            else:
                local_coef = np.interp(x=d, xp=xlist, fp=sclist)
                local_storage = local_coef * dstep * 1000
                local_storage_sum = local_storage_sum + local_storage

            # calculate storage in rooth zone
            rooth_storage = np.interp(x=d, xp=xlist, fp=rzlist)
            avail_rooth_storage = max_soil_rooth_storage - rooth_storage

            # Total availeble storage
            total_storage = (avail_rooth_storage + local_storage_sum) / 1000

            storage_lookup_dict.append(
                {
                    'Soil Type': soil_type,
                    'Rootzone Thickness (cm)': rootzone_thickness,
                    'Dewathering Depth (m)': d,
                    'Storage Coefficient (m/m)': local_coef,
                    'Local Storage (mm)': local_storage,
                    'Unsa Zone Storage (mm)': local_storage_sum,
                    'Local Total Rooth Zone Storage (mm)': rooth_storage,
                    'Available Rooth Zone Storage (mm)': avail_rooth_storage,
                    'Total Available Storage (m)': total_storage
                }
            )

    return pd.DataFrame(storage_lookup_dict)
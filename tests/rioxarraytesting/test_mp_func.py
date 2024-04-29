def create_depth(scenario):
    print(scenario.name)
    threedi_result = scenario.netcdf

    # klimaatsommenprep.calculate_depth(
    #     scenario=scenario,
    #     threedi_result=threedi_result,
    #     grid_filename="grid_wlvl.gpkg",
    #     overwrite=True,
    # )

    return scenario.name


def worker(i, r):
    print(r)
    return r[0] * r[0]

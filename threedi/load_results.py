from threedigrid.admin.gridresultadmin import GridH5ResultAdmin

def load_threedi_results(test_env):
    '''
    Gets result information from nc and h5 files
    '''
    nc_file = test_env.paths['nc_file']
    h5_file = test_env.paths['h5_file']
    try:
        result = GridH5ResultAdmin(h5_file_path=h5_file,
                                   netcdf_file_path=nc_file)
        return result
    except Exception as e:
        raise e from None

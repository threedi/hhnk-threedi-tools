from threedigrid.admin.gridresultadmin import GridH5ResultAdmin
# threedigrid.admin.gridresultadmin.logging.disable()
import threedigrid
import h5py

h5_path = r'C:\Users\bramv\Desktop\HHNK\rtc_heerhugowaard\03.3di_results\0d1d_results\RTC Heerhugowaard #13 0d1d_test\gridadmin.h5'
nc_path = r'C:\Users\bramv\Desktop\HHNK\rtc_heerhugowaard\03.3di_results\0d1d_results\RTC Heerhugowaard #13 0d1d_test\results_3di.nc'

res = GridH5ResultAdmin(h5_path, nc_path)
print(dir(res.lines))
print(threedigrid.__version__)
print(h5py.__version__)
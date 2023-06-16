# %%
import sys
import os

sys_paths = [
    fr"E:\github\{os.getlogin()}\hhnk-threedi-tools",
    fr"E:\github\{os.getlogin()}\hhnk-research-tools",
]
for sys_path in sys_paths:
    if sys_path not in sys.path:
        if os.path.exists(sys_path):
            sys.path.insert(0, sys_path)


remove_paths = [fr'C:/Users/{os.getlogin()}/AppData/Roaming/3Di/QGIS3/profiles/default/python/plugins/hhnk_threedi_plugin/external-dependencies',
 fr'C:/Users/{os.getlogin()}/AppData/Roaming/3Di/QGIS3/profiles/default/python/plugins/ThreeDiToolbox/deps']

for sys_path in remove_paths:
    if sys_path in sys.path:
        sys.path.remove(sys_path)
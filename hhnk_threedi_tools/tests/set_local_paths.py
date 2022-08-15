import sys
sys_paths = [r'E:\github\wvangerwen\hhnk-threedi-tools', r'E:\github\wvangerwen\hhnk-research-tools']
for sys_path in sys_paths:
    if sys_path not in sys.path:
        sys.path.insert(0,sys_path)
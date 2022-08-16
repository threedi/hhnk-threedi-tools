import os
def read_api_file(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            api_key = f.readline()
        return api_key
    else:
        return ''
import os
import json

def read_api_file(file_path):
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                api_keys = f.read()
            return  json.loads(api_keys)
            
        except Exception as e:
            raise e
        
    else:
        return {"lizard":"", "threedi":""}
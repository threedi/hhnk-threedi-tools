import os
from .build_folder_structure_dict import build_folder_structure_dict

def create_new_project_folder(base_path):
    try:
        dict = build_folder_structure_dict(polder_path=base_path)
        for item in dict.values():
            os.makedirs(item, exist_ok=True)
    except Exception as e:
        raise e from None

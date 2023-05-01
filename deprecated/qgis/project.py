# -*- coding: utf-8 -*-
"""
Created on Fri Nov 19 10:53:45 2021

@author: chris.kerklaan
"""
import shutil
import pathlib
import os

current_path = str(pathlib.Path(__file__).parent.absolute())
klimaatsommen_qgis_project = current_path + "/qgis3_export_pdfs.qgz"


def copy_projects(new_dir, original_dir=current_path):
    os.makedirs(new_dir, exist_ok=True)

    for file in os.listdir(original_dir):
        if file.endswith(".qgz"):
            shutil.copy2(original_dir + "/" + file, new_dir + "/" + file)

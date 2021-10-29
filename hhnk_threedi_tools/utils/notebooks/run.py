# -*- coding: utf-8 -*-
"""
Created on Fri Sep 24 13:55:39 2021

@author: chris.kerklaan

Opens a jupyter notebook based on nbopen using the command line

"""
import os
import sys
import subprocess
import pathlib


def open_notebook(filename):
    """opens a jupyter notebook using nbopen"""
    
    
    ipy_dir = pathlib.Path(__file__).parent

    if not  '.ipynb' in filename:
        filename = filename + '.ipynb'
        
    _run_notebook(str(ipy_dir / filename))


def _get_python_interpreter():
    """Return the path to the python3 interpreter.

    Under linux sys.executable is set to the python3 interpreter used by Qgis.
    However, under Windows/Mac this is not the case and sys.executable refers to the
    Qgis start-up script.
    """
    interpreter = None
    executable = sys.executable
    directory, filename = os.path.split(executable)
    if 'python' in filename: 
    
        if filename.lower() in ["python.exe", "python3.exe"]:
            interpreter = executable
        else:
            raise EnvironmentError("Unexpected value for sys.executable: %s" % executable)
        assert os.path.exists(interpreter)  # safety check
        return "python", interpreter
    
    elif 'qgis' in filename:
        # qgis python interpreter
        main_folder = str(pathlib.Path(executable).parents[0])
        folder_files = os.listdir(main_folder)
        
        if "py3_env.bat" in folder_files:
            interpreter = main_folder + "/py3_env.bat"
        
        if "python-qgis-ltr.bat" in folder_files:
            interpreter = main_folder + "/python-qgis-ltr.bat"
            
        if not interpreter:
            raise EnvironmentError("could not find qgis-python bat file in: %s" % main_folder)
            
        return "qgis", interpreter

def _run_notebook(notebook_path):
    system, python_interpreter = _get_python_interpreter()
    if system == "qgis":
        command = [python_interpreter, "-m", "notebook", notebook_path]
    else:
        command = [python_interpreter, "-m", "jupyter", "notebook", notebook_path]
    
    process = subprocess.Popen(
        command,
        universal_newlines=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    
    
def open_server():
    system, python_interpreter = _get_python_interpreter()
    if system == "qgis":
        command = [python_interpreter, "-m", "notebook"]
    else:
        command = [python_interpreter, "-m", "jupyter", "notebook"]
    
    process = subprocess.Popen(
        command,
        universal_newlines=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    
    
    
    # i, o, e = (process.stdin, process.stdout, process.stderr)
    # i.close()
    # result = o.read() + e.read()
    # o.close()
    # e.close()
    # print(result)
    # exit_code = process.wait()
    # if exit_code:
    #     raise RuntimeError("Notebook failed")




if __name__ == "__main__":
    open_notebook("02_calculation_gui.ipynb")

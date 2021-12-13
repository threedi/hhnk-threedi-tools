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
import tempfile
import shutil
import json
import site
import multiprocessing

CREATE_NEW_PROCESS_GROUP = 0x00000200
DETACHED_PROCESS = 0x00000008

NOTEBOOK_DIRECTORY = str(pathlib.Path(__file__).parent.absolute())


class TempCopy:
    def __init__(self, original_path):
        self.original_path = original_path

    def __enter__(self):
        temp_dir = tempfile.gettempdir()
        base_path = os.path.basename(self.original_path)
        self.path = os.path.join(temp_dir, base_path)
        shutil.copy2(self.original_path, self.path)
        return self.path

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.remove(self.path)


def copy_notebooks(new_dir, original_dir=NOTEBOOK_DIRECTORY):
    os.makedirs(new_dir, exist_ok=True)

    for file in os.listdir(original_dir):
        if file.endswith(".ipynb"):
            shutil.copy2(original_dir + "/" + file, new_dir + "/" + file)


def write_notebook_json(directory, data):
    with open(directory + "/notebook_data.json", "w") as f:
        json.dump(data, f)


def read_notebook_json(directory):
    with open(directory + "/notebook_data.json") as f:
        return json.load(f)


def open_notebook(filename, temp=False):
    """Creates a copy and opens a jupyter notebook using nbopen"""

    ipy_dir = pathlib.Path(__file__).parent

    if not ".ipynb" in filename:
        filename = filename + ".ipynb"

    assert os.path.exists(str(ipy_dir / filename))

    if temp:
        with TempCopy(str(ipy_dir / filename)) as temp_copy_path:
            print("Opening copy", temp_copy_path)
            _run_notebook(temp_copy_path)
    else:
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
    if "python" in filename:

        if filename.lower() in ["python.exe", "python3.exe"]:
            interpreter = executable
        else:
            raise EnvironmentError(
                "Unexpected value for sys.executable: %s" % executable
            )
        assert os.path.exists(interpreter)  # safety check
        return "python", interpreter

    elif "qgis" in filename:
        # qgis python interpreter
        main_folder = str(pathlib.Path(executable).parents[0])
        folder_files = os.listdir(main_folder)

        if "py3_env.bat" in folder_files:
            interpreter = main_folder + "/py3_env.bat"

        if "python-qgis-ltr.bat" in folder_files:
            interpreter = main_folder + "/python-qgis-ltr.bat"

        if not interpreter:
            raise EnvironmentError(
                "could not find qgis-python bat file in: %s" % main_folder
            )

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

    # with process.stdout:
    #     for line in iter(process.stdout.readline, b''):
    #         print(line)

    # process.wait()


def copy_sys_env():
    myenv = os.environ.copy()
    myenv["PYTHONPATH"] = ";".join([str(i) for i in sys.path])
    return myenv


def user_installed_notebook_path():
    path = site.getusersitepackages().replace("site-packages", "Scripts")
    if os.path.exists(path + "/jupyter-notebook.exe"):
        return path + "/jupyter-notebook.exe"
    else:
        return None


def notebook_command(location="osgeo"):
    """if jupyter is installed in osgeo, use osgeo.
    if jupyter is installed in the user dir 'pip install jupyter --user'
    use 'user'.

    'user' uses an exectuable
    """
    if location == "osgeo":
        system, python_interpreter = _get_python_interpreter()
        if system == "qgis":
            command = [python_interpreter, "-m", "notebook"]
        else:
            command = [python_interpreter, "-m", "jupyter", "notebook"]
    else:
        command = [user_installed_notebook_path()]
    return command


def open_server(directory=None, location="osgeo", use="run"):
    """directory:
        notebooks open in a certain directory
    location:
        can either be osgeo or user
        open jupyter notebook is osgeo or per-user-installed .exe
    use:
        subprocess mode ('popen' or 'run')
    """
    command = notebook_command(location)

    if directory:
        command.append(directory)

    if use == "popen":
        process = subprocess.Popen(
            command,
            shell=True,
            universal_newlines=True,
            stdin=None,
            stdout=None,
            stderr=None,
            close_fds=True,
            env=copy_sys_env(),
            creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP,
        )
        print(f"Started processing with pid: {process.pid} and command {command}")
    elif use == "run":
        command = ["start", "cmd", "/K"] + command
        process = subprocess.run(command, shell=True)
        print(f"Started processing with command {command}")
    


def create_command_bat_file(path, location="osgeo"):
    command = notebook_command(location)

    with open(path, "w") as bat_file:
        bat_file.write(" ".join(command))

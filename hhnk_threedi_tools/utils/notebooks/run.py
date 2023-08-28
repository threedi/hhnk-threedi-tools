# -*- coding: utf-8 -*-
# %%
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
from pathlib import Path

CREATE_NEW_PROCESS_GROUP = 0x00000200
DETACHED_PROCESS = 0x00000008

NOTEBOOK_DIRECTORY = str(pathlib.Path(__file__).parent.absolute())


class TempCopy:
    def __init__(self, original_path):
        self.original_path = Path(original_path)

    def __enter__(self):
        temp_dir = tempfile.gettempdir()
        base_path = self.original_path.name
        self.path = os.path.join(temp_dir, base_path)
        shutil.copy2(self.original_path, self.path)
        return self.path

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.remove(self.path)


def copy_notebooks(new_dir, original_dir=NOTEBOOK_DIRECTORY):
    os.makedirs(new_dir, exist_ok=True)

    for file in os.listdir(original_dir):
        print(file)
        if file.endswith(".ipynb"):
            cont=True
        elif file == "notebook_setup.py":
            cont=True
        else:
            cont=False

        if cont:
            shutil.copy2(original_dir + "/" + file, new_dir + "/" + file)


def write_notebook_json(directory, data):
    with open(directory + "/notebook_data.json", "w") as f:
        json.dump(data, f)


def read_notebook_json(directory):
    with open(directory + "/notebook_data.json") as f:
        return json.load(f)


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


def user_installed_notebook_path():
    path = site.getusersitepackages().replace("site-packages", "Scripts")
    if os.path.exists(path + "/jupyter-lab.exe"):
        return path + "/jupyter-lab.exe"
    else:
        return None


def user_installed_ipython_path():
    path = site.getusersitepackages().replace("site-packages", "Scripts")
    if os.path.exists(path + "/ipython.exe"):
        return path + "/ipython.exe"
    else:
        return None


def notebook_command(location="osgeo", ipython=False):
    """if jupyter is installed in osgeo, use osgeo.
    if jupyter is installed in the user dir 'pip install jupyter --user'
    use 'user'.

    'user' uses an exectuable
    """
    system, python_interpreter = _get_python_interpreter()
    if location == "osgeo":
        command = [python_interpreter, "-m", "jupyter-lab"]

        # if system == "qgis":
        #     command = [python_interpreter, "-m", "notebook"]
        # else:
        #     command = [python_interpreter, "-m", "jupyter", "notebook"]
    else:
        if ipython:
            command = [python_interpreter, user_installed_ipython_path()]
        else:
            command = [python_interpreter, user_installed_notebook_path()]
    return command

def open_server(directory=None, location="osgeo", use="run", notebook_paths=[]):
    """directory:
        notebooks open in a certain directory
    location:
        can either be 'osgeo' or 'user'
        open jupyter notebook is osgeo or per-user-installed .exe
    use:
        subprocess mode ('popen' or 'run')
    """
    paths = [Path(i) for i in os.environ.get("PATH").split(os.pathsep)]
    for path in notebook_paths:
        path = Path(path)
        if not path in paths:
            os.environ["PATH"] = f"{path.as_posix()}{os.pathsep}{os.environ['PATH']}"
    print(f"path :{os.environ.get('PATH').split(os.pathsep)}")
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


#TODO this doesnt work nicely with other  environments. Prepare for deprecation
def add_notebook_paths(extra_notebook_paths):
    """adds extra notebook paths, which is used in the plugin"""

    # user profile paths
    user_profile_path = os.environ["USERPROFILE"]
    ipython_profile_path = (
        user_profile_path + "/.ipython/profile_default/ipython_config.py"
    )

    nb_path_command = "import sys"
    for path in extra_notebook_paths:
        path = path.replace("\\", "/")
        nb_path_command = nb_path_command + f'; sys.path.insert(0,"{path}")'
    nb_string = f"c.InteractiveShellApp.exec_lines = ['{nb_path_command}']"

    if not os.path.exists(ipython_profile_path):
        # create a profile
        command = notebook_command("user", ipython=True)
        command = command[1] + " profile create"

        subprocess.run(command, shell=True)
        print("Creating profile with: ", command)
        # print(["start", "cmd", "/K"] + command)
        # output, error = process.communicate()
        # exit_code = process.wait()
        # if exit_code:
        #     print(f"Creating ipython profile failed: {error} {output}")

    # check if paths are already available
    exists = False
    with open(ipython_profile_path, "r") as profile_code:
        for i in profile_code:
            if nb_string in profile_code:
                exists = True
                break

    if not exists:
        print("Adding:", nb_string)
        with open(ipython_profile_path, "a") as profile_code:
            profile_code.write("\n" + nb_string)

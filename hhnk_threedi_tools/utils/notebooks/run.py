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
    if filename.lower() in ["python.exe", "python3.exe"]:
        interpreter = executable
    else:
        raise EnvironmentError("Unexpected value for sys.executable: %s" % executable)
    assert os.path.exists(interpreter)  # safety check
    return interpreter


def _run_notebook(notebook_path):
    python_interpreter = _get_python_interpreter()
    command = [python_interpreter, "-m" "nbopen", notebook_path]

    process = subprocess.Popen(
        command,
        universal_newlines=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # The input/output/error stream handling is a bit involved, but it is
    # necessary because of a python bug on windows 7, see
    # https://bugs.python.org/issue3905 .
    i, o, e = (process.stdin, process.stdout, process.stderr)
    i.close()
    result = o.read() + e.read()
    o.close()
    e.close()
    exit_code = process.wait()
    if exit_code:
        raise RuntimeError(f"Opening {notebook_path} failed")


if __name__ == "__main__":
    open_notebook("02_calculation_gui.ipynb")

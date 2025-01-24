REM force-exclude to make sure it uses the extend-exclude from pyproject.
python -m ruff check ../hhnk_threedi_tools --select I --fix 
python -m ruff format ../hhnk_threedi_tools --force-exclude
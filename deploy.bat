REM install local folder to external deps
python setup.py bdist_wheel 
pip uninstall hhnk_threedi_tools -y
pip install --target %appdata%\3Di\QGIS3\profiles\default\python\plugins\hhnk_threedi_plugin\external-dependencies --upgrade dist\hhnk_threedi_tools-2023.3-py3-none-any.whl


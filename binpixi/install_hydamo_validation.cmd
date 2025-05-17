@ECHO OFF

IF NOT EXIST "..\HyDAMOValidatieModule" (
    git clone https://github.com/HHNK/HyDAMOValidatieModule/ ..\HyDAMOValidatieModule
)
pip install --no-deps -e ..\HyDAMOValidatieModule
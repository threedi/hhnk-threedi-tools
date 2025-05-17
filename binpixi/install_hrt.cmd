@ECHO OFF

IF NOT EXIST "..\hhnk-research-tools" (
    git clone https://github.com/HHNK/hhnk-research-tools/ ..\hhnk-research-tools
)
pip install --no-deps -e ..\hhnk-research-tools
@ECHO OFF

@REM IF NOT EXISTS "..\hhnk-theedi-tools"
@REM htt = "pip install --no-build-isolation --no-deps --disable-pip-version-check -e ../hhnk-threedi-tools"

IF NOT EXIST "..\hhnk-research-tools" (
    git clone https://github.com/HHNK/hhnk-research-tools/ ..\hhnk-research-tools
)
pip install --no-deps -e ..\hhnk-research-tools
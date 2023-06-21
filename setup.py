# %%
from setuptools import setup, find_packages
import codecs
import re
import os

here = os.path.abspath(os.path.dirname(__file__))

def read(*parts):
    with codecs.open(os.path.join(here, *parts), "r") as fp:
        return fp.read()

def find_version(*file_paths):
    """
    Search the file for a version string.
    file_path contain string path components.
    Reads the supplied Python module as text without importing it.
    """
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    name="hhnk-threedi-tools",
    version=find_version("hhnk_threedi_tools", "__init__.py"),
    description="HHNK watersystemen analysis tools",
    url="https://github.com/threedi/hhnk-threedi-tools",
    author="Wietse van Gerwen, Laure Ravier",
    author_email="w.vangerwen@hhnk.nl",
    maintainer="Wietse van Gerwen",
    project_urls={
        "Bug Tracker": "https://github.com/threedi/hhnk-threedi-tools/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    # package_dir={'':'hhnk_threedi_tools'},
    # packages=find_packages(),
    packages=find_packages(exclude=['tests', 'deprecated']),
    python_requires=">=3.7",
    # install_requires=["hhnk-research-tools==2023.1", "xarray", "pytest"],
    # setup_requires=['setuptools_scm'],
    include_package_data=True,
)

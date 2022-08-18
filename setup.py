from setuptools import setup, find_packages

setup(
    name="hhnk-threedi-tools",
    version="0.9.2",
    description="HHNK watersystemen analysis tools",
    url="https://github.com/threedi/hhnk-threedi-tools",
    author="Wietse van Gerwen, Laure Ravier",
    author_email="w.vangerwen@hhnk.nl",
    maintainer="Chris Kerklaan",
    project_urls={
        "Bug Tracker": "https://github.com/threedi/hhnk-threedi-tools/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    # package_dir={'':'hhnk_threedi_tools'},
    packages=find_packages(),
    # packages=find_packages("", exclude=['tests']),
    python_requires=">=3.7",
    install_requires=["hhnk-research-tools==0.8"],
    # setup_requires=['setuptools_scm'],
    include_package_data=True,
)

from setuptools import setup, find_packages

setup(
    name='hhnk_wsa_tests',
    version='0.1.2',    
    description='HHNK watersystemen analyse tests',
    url='https://github.com/LER1990/hhnk_toolbox_universal',
    author='Laure Ravier',
    author_email='L.Ravier@hhnk.nl',
    project_urls={
        "Bug Tracker": "https://github.com/LER1990/hhnk_toolbox_universal/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(),
    python_requires=">=3.7",
	install_requires=[
		'hhnk_threedi_tools==0.1.4'
	]
	# handled in hhnk_threedi_tools
	#    	'numpy>=1.19.1',
	# 'Shapely>=1.7.0',
	# 'gdal>=3.1.4',
    # 'pandas>=1.0.1',
    # 'geopandas>=0.7.0',
    # 'threedigrid>=1.0.25',
)

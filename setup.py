from setuptools import setup, find_packages

setup(
    name='hhnk_threedi_tests',
    version='0.1.3',
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
		'hhnk_research_tools==0.1.1'
	]
)

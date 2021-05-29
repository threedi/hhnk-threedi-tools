from setuptools import setup, find_packages

setup(
    name='hhnk-threedi-tests',
    version='0.1.9',
    description='HHNK watersystemen analysis tests',
    url='https://github.com/wvangerwen/hhnk-threedi-tests',
    author='Laure Ravier',
    author_email='L.Ravier@hhnk.nl',
    project_urls={
        "Bug Tracker": "https://github.com/wvangerwen/hhnk-threedi-tests/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(),
    python_requires=">=3.7",
	install_requires=[
		'hhnk-research-tools>=0.2.2'
	]
)

from setuptools import setup, find_packages
from pathlib import Path

this_directory= Path(__file__).parent
long_description = (this_directory / "PyPI_README.md").read_text()

setup(
    name="sparkplus",
    version="1.2.1",
    description="GIS package for Apache Spark",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="sparkplus",
    author_email="meadea27@gmail.com",
    url="https://github.com/SWM-SparkPlus/sparkplus",
    license="MIT",
    # py_modules=['conversion', 'load_database'],
    python_requires=">=3",
    install_requires=[
        "numpy",
        "pandas",
        "geopandas",
        "geospark",
        "h3",
        "geopy",
        "pyarrow",
        "rtree",
        "shapely",
        "python-dotenv",
    ],
    include_package_data=True,
    zip_safe=False,
    packages=find_packages(),
    keywords=["spark", "geo"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)

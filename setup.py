from setuptools import setup, find_packages

setup(
    name="sparkplus",
    version="1.0.0",
    description="GIS package for sparkplus",
    author="sparkplus",
    author_email="meadea27@gmail.com",
    url="https://github.com/SWM-SparkPlus/spark-plugin",
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

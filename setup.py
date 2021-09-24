from setuptools import setup, find_packages

setup(
    name='sparkplus-test',
    version="0.1",
    description='test package for sparkplus',
    author='sparkplus',
    author_email='meadea27@gmail.com',
    url='https://github.com/SWM-SparkPlus/spark-plugin',
    license='MIT',
    py_modules=['conversion', 'load_database'],
    python_requires='>=3',
    install_requires=[
        'numpy', 
        'pandas', 
        'geopandas', 
        'geospark', 
        'h3', 
        'geopy', 
        'pyarrow', 
        'rtree', 
        'shapely'],
    package=find_packages(exlcude=[]),
    keywords=['spark', 'geo'],
    classifiers         = [
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)

setuptools.setup(
    name="spark-plus",
    version="1.0",
    license="MIT",
    author="spark-plus",
    author_email="meadea27@gmail.com",
    description="plugin for analyzing Korean address system",
    # long_description=open('README.md').read(),
    # url="github url 등",
    packages=setuptools.find_packages(),
    classifiers=[
        # 패키지에 대한 태그
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)

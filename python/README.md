# Delta Lake

[Delta Lake](https://delta.io) is an open source storage layer that brings reliability to data lakes. Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. Delta Lake runs on top of your existing data lake and is fully compatible with Apache Spark APIs.

This PyPi package contains the Python APIs for using Delta Lake with Apache Spark.

## Installation and usage

1. Install using `pip install delta-spark`
2. To use the Delta Lake with Apache Spark, you have to set additional configurations when creating the SparkSession. See the online [project web page](https://docs.delta.io/latest/delta-intro.html) for details.

## Documentation

This README file only contains basic information related to pip installed Delta Lake. You can find the full documentation on the [project web page](https://docs.delta.io/latest/delta-intro.html)

## Running python tests locally

## Setup Environment
### Install Conda (Skip if you already installed it)
Follow [Conda Download](https://www.anaconda.com/download/) to install Anaconda.

### Create an environment from environment file
Follow [Create Environment From Environment file](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-from-file) to create a Conda environment from `<repo-root>/python/environment.yml` and activate the newly created `delta_python_tests` environment.

```
# Note the `--file` argument should be a fully qualified path. Using `~` in file
# path doesn't work. Example valid path: `/Users/macuser/delta/python/environment.yml`

conda env create --name delta_python_tests --file=<absolute_path_to_delta_repo>/python/environment.yml`
```

### JDK Setup
Build needs JDK 1.8. Make sure to setup `JAVA_HOME` that points to JDK 1.8.

### Running tests
```
conda activate delta_python_tests
python3 <delta-root>/python/run-tests.py
```


# Docs Generation Scripts
This directory contains scripts to generate API Docs for Scala, Java, and Python.

## Setup Environment
### Install Conda (Skip if you already installed it)
Follow [Conda Installation Guide](https://docs.conda.io/projects/continuumio-conda/en/latest/user-guide/install/macos.html) to install Anaconda.

### Create an environment from environment file
Follow [Create Environment From Environment file](https://docs.conda.io/projects/conda/en/4.6.1/user-guide/tasks/manage-environments.html#create-env-from-file) to create a Conda environment from `/delta/docs/environment.yml` and activate the newly created `docs` environment.

### Run Script
Run `python3 delta/docs/generate_api_docs.py` to generate API docs.

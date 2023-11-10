# Docs Generation Scripts
This directory contains scripts to generate API Docs for Scala, Java, and Python.

## Setup Environment
### Install Conda (Skip if you already installed it)
Follow [Conda Download](https://www.anaconda.com/download/) to install Anaconda.

### Create an environment from environment file
Follow [Create Environment From Environment file](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-from-file) to create a Conda environment from `/delta/docs/environment.yml` and activate the newly created `delta_docs` environment.

```
# Note the `--file` argument should be a fully qualified path. Using `~` in file
# path doesn't work. Example valid path: `/Users/macuser/delta/docs/environment.yml`

conda env create --name delta_docs --file=<absolute_path_to_delta_repo>/docs/environment.yml`
```

### Run Script
Run `python3 <delta root>/docs/generate_api_docs.py` to generate API docs.

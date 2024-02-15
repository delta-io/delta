# Docs Generation Scripts
This directory contains scripts to generate docs for `https://docs.delta.io/`
including the API Docs for Scala, Java, and Python APIs.

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

### JDK Setup
API doc generation needs JDK 1.8. Make sure to setup `JAVA_HOME` that points to JDK 1.8.

### Generate docs
Run the command from the `delta` repo root directory:
```
python3 docs/generate_docs.py --livehtml --api-docs
```
Above command will print a URL to preview the docs.

### Skip generating API docs
Above command generates API docs which take time. If you are just interested in the docs
that go on https://docs.delta.io, use the following command.
```
python3 docs/generate_docs.py --livehtml
```


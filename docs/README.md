# Docs Generation Scripts

This directory contains scripts to generate docs for https://docs.delta.io, including the API Docs for Scala, Java, and Python APIs.

## Setup Environment

### Install Node environment

Install node v22.14.0 using [nvm](https://github.com/nvm-sh/nvm):

```
nvm install
```

Then, install [pnpm](https://pnpm.io/):

```
npm install --global corepack@latest
corepack enable pnpm
```

Finally, install dependencies:

```
pnpm i
```

### Install Conda environment

Follow [Conda Download](https://www.anaconda.com/download/) to install Anaconda.

Then, follow [Create Environment From Environment file](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-from-file) to create a Conda environment from `/delta/docs/environment.yml` and activate the newly created `delta_docs` environment.

```
# Note the `--file` argument should be a fully qualified path. Using `~` in file
# path doesn't work. Example valid path: `/Users/macuser/delta/docs/environment.yml`

conda env create --name delta_docs --file=<absolute_path_to_delta_repo>/docs/environment.yml`
```

### JDK Setup

API doc generation needs JDK 1.8. Make sure to setup `JAVA_HOME` that points to JDK 1.8.

### Set the Delta Lake version

Set the version of Delta Lake release these docs are being generated for.

```
export _DELTA_LAKE_RELEASE_VERSION_=3.3.0
```

## Usage

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

### Building for production

To build the docs for production, run the following command:

```python
python3 docs/generate_docs.py --api-docs
```

The resulting files will be found in `docs/dist`.

### Additional docs site commands

The docs site is built on [Astro](https://astro.build/). Using pnpm, you can run a variety of commands:

| Command               | Description                                     |
| --------------------- | ----------------------------------------------- |
| `pnpm run lint`       | Run ESLint on the docs site code                |
| `pnpm run format`     | Format docs site code using Prettier            |
| `pnpm run dev`        | Start Astro in development mode                 |
| `pnpm run build`      | Build the Astro site for production             |
| `pnpm run preview`    | Preview the built Astro site                    |
| `pnpm run astro`      | Run Astro CLI                                   |

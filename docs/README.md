# Delta Docs

This folder contains all of the source code needed to generate the delta documentation site.

## Getting started

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

## Usage

The docs site is build on [Astro](https://astro.build/). Using pnpm, you can run a variety of commands:

| Command               | Description                                     |
| --------------------- | ----------------------------------------------- |
| `pnpm run lint`       | Run ESLint on the docs site code                |
| `pnpm run format`     | Format docs site code using Prettier            |
| `pnpm run dev`        | Start Astro in development mode                 |
| `pnpm run build`      | Build the Astro site for production             |
| `pnpm run build:apis` | Build API docs (runs apis/generate_api_docs.py) |
| `pnpm run preview`    | Preview the built Astro site                    |
| `pnpm run astro`      | Run Astro CLI                                   |

## Building API docs

API docs are built separately. Follow these steps to build API docs.

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

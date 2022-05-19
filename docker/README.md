# Docker Instructions

## Building and running the image

The following will build with the repo root as the build context, where you may supply whatever image name you prefer

```bash
docker build -t <image_name> -f Dockerfile ..
```

Example process after cloning repo:

```bash
cd docker
docker build -t delta_docker -f Dockerfile ..
docker run --rm -it delta_docker
```


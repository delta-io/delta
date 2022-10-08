# Delta Lake Quickstart Container

A docker deployment of Spark with Delta Lake


## Docker High Level Instructions

- clone this repo onto a resource running docker engine
- navigate to the cloned folder
- use the commands in this file to build the image, run the container, and navigate to the Jupyter Lab service
- open and follow the quickstart notebook and/or exec into a container shell and follow scala/python instructions there and/or open a terminal in jupyter hub and follow the scala/python instructions there

## Docker build/run Instructions

- open a bash shell (if on windows use git bash, WSL, or any shell configged for bash commands)
- execute build command from same folder as docker file
- exectute run command
- get the log tail and navigate to the last url displayed

## Docker Commands

### Build

```bash
cd quickstart_docker
docker build -t delta_quickstart -f Dockerfile_quickstart .
```

### Run

```bash
docker run -d --rm -p 8888-8889:8888-8889 delta_quickstart 
```

### Exec into a shell
```bash
docker exec -it <first 4 containerid> bash
```

### Viewing Logs

Get the tail
```bash
docker logs <first 4 containerid>
```

Get continuous logs

```bash
docker logs --follow <fist 4 container id>
```
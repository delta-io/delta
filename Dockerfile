FROM python:3.7.3-stretch

RUN apt-get update && apt-get -y install openjdk-8-jdk

RUN pip install pyspark==3.2.0

RUN pip install mypy==0.910

RUN pip install importlib_metadata==3.10.0

COPY .  /usr/src/delta

WORKDIR /usr/src/delta

CMD ["python", "run-tests.py"]

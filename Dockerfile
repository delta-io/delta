FROM python:3.7.3-stretch

RUN apt-get update && apt-get -y install openjdk-8-jdk

RUN pip install https://docs.delta.io/spark3artifacts/snapshot-740da348/distributions/pyspark-3.0.0.tar.gz

COPY .  /usr/src/delta

WORKDIR /usr/src/delta

CMD ["python", "run-tests.py"]

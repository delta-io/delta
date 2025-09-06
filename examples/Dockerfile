FROM jupyter/minimal-notebook

ARG PYTHON_VERSION=3.9
ARG SCALA_VERSION=2.12
ARG DELTA_LAKE_VERSION=1.1.0
ARG SPARK_VERSION=3.2.1
ARG DELTA_PACKAGE=io.delta:delta-core_${SCALA_VERSION}:${DELTA_LAKE_VERSION}

USER root

RUN apt-get update && \
    apt-get -y install openjdk-8-jdk r-base \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER $NB_UID

ENV JUPYTER_TOKEN="docker"
ENV JUPYTER_ENABLE_LAB="yes"

RUN pip install delta-spark==${DELTA_LAKE_VERSION}

RUN pip install jupytext

COPY --chown=$NB_UID:$NB_UID python python/
COPY --chown=$NB_UID:$NB_UID tutorials tutorials/

RUN mkdir -p notebooks

RUN find python/ -name "*.py" -exec basename {} .py ';' | \
    xargs -I {} jupytext --to .ipynb -o "notebooks/{}.ipynb" "python/{}.py"

ENV SPARK_HOME=/opt/conda/lib/python${PYTHON_VERSION}/site-packages/pyspark
ENV PATH=${PATH}:/opt/conda/lib/python${PYTHON_VERSION}/site-packages/pyspark/bin

RUN mkdir $SPARK_HOME/conf

RUN echo "spark.jars.packages ${DELTA_PACKAGE}" > $SPARK_HOME/conf/spark-defaults.conf
RUN echo "spark.sql.extensions  io.delta.sql.DeltaSparkSessionExtension" >> $SPARK_HOME/conf/spark-defaults.conf
RUN echo "spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog" >> $SPARK_HOME/conf/spark-defaults.conf

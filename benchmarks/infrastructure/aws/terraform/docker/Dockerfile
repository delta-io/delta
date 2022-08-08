ARG SPARK_VERSION=v3.2.1
FROM apache/spark:$SPARK_VERSION

ARG MARIADB_DRIVER_VERSION=2.7.2

USER 0

RUN ln -s /opt/spark/bin/spark-submit /bin/spark-submit
RUN ln -s /opt/spark/bin/spark-shell /bin/spark-shell

RUN apt update
RUN apt install --assume-yes wget
# mariadb driver is needed to communicate with Hive Metastore.
RUN wget https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/${MARIADB_DRIVER_VERSION}/mariadb-java-client-${MARIADB_DRIVER_VERSION}.jar -P /opt/spark/jars/

USER 185

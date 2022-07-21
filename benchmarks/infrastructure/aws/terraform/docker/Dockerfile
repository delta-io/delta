FROM apache/spark:v3.2.1

USER 0

RUN ln -s /opt/spark/bin/spark-submit /bin/spark-submit
RUN ln -s /opt/spark/bin/spark-shell /bin/spark-shell

RUN apt update
RUN apt install --assume-yes wget
RUN wget https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/2.7.2/mariadb-java-client-2.7.2.jar -P /opt/spark/jars/

USER 185

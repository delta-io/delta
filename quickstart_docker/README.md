# What is this?

This folder contains instructions and materials to get new users started with Delta Lake and work through the quickstart materials.  Follow the steps below to build an Apache Spark image with Delta Lake intalled, run a container, and follow the quickstart in an interactive notebook or shell.

1. [Build the image](#Build-the-Image)
2. [Choose an interface](#Choose-an-Interface)

## Build the Image

1. Clone this repo
2. Navigate to the cloned folder
3. Navigate to the quickstart_docker folder
4. open a bash shell (if on windows use git bash, WSL, or any shell configured for bash commands)
5. Execute the following

    ```bash
    cd quickstart_docker
    docker build -t delta_quickstart -f Dockerfile_quickstart .
    ```

Once the image has been built, you can then move on to running the quickstart in a notebook or shell.

## Choose an Interface

- [Pyspark Jupyter Lab Notebook](#Pyspark-Jupyter-Lab-Notebook)
- [Pyspark Shell](#Pyspark-Shell)
- [Scala Shell](#Scala-Shell)

### Jupyter Lab Notebook

1. Open a bash shell (if on windows use git bash, WSL, or any shell configured for bash commands)
2. Run a container from the built image with a Juypter Lab entrypoint

    ```bash
    docker run --rm -it -p 8888-8889:8888-8889 delta_quickstart 
    ```

3. Follow or cut/paste the Jupyter Lab
4. Open the quickstart notebook and follow along

**Note that you may also use launch the pyspark or scala shells after launching a terminal in Jupyter Lab**

### Pyspark Shell

1. Open a bash shell (if on windows use git bash, WSL, or any shell configured for bash commands)
2. Run a container from the built image with a bash entrypoint 

    ```bash
    docker run --rm -it --entrypoint bash delta_quickstart
    ```
3. Launch a pyspark interactive shell session

    ```bash
    $SPARK_HOME/bin/pyspark --packages io.delta:${DELTA_PACKAGE_VERSION} \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    ```

4. Run some basic commands in the shell

    ```python
    # Create Spark DataFrame
    data = spark.range(0, 5)
    data.write.format("delta").save("/tmp/delta-table")

    # Read Delta table
    df = spark.read.format("delta").load("/tmp/delta-table")                    

    # Show Delta table
    df.show()
    ```

5.  Continue with the quickstart [here](https://docs.delta.io/latest/quick-start.html#create-a-table&language-python)

To verify that you have a Delta table, you can list the contents within the folder of your Delta table. For example, in the previous code, you saved the table in /tmp/delta-table. Once you close your pyspark process, run a list command in your Docker shell and you should get something similar to below.

```bash
$ ls -lsgA /tmp/delta-table
total 36
4 drwxr-xr-x 2 NBuser 4096 Oct 18 02:02 _delta_log
4 -rw-r--r-- 1 NBuser  478 Oct 18 02:02 part-00000-b968d89a-b299-401f-a6db-ba0c160633ab-c000.snappy.parquet
4 -rw-r--r-- 1 NBuser   12 Oct 18 02:02 .part-00000-b968d89a-b299-401f-a6db-ba0c160633ab-c000.snappy.parquet.crc
4 -rw-r--r-- 1 NBuser  478 Oct 18 02:02 part-00001-f0f8ea27-b522-4c2c-8fe3-7224fccacb91-c000.snappy.parquet
4 -rw-r--r-- 1 NBuser   12 Oct 18 02:02 .part-00001-f0f8ea27-b522-4c2c-8fe3-7224fccacb91-c000.snappy.parquet.crc
4 -rw-r--r-- 1 NBuser  478 Oct 18 02:02 part-00002-b8a1ea0d-0637-4432-8ab6-8ec864edb6b0-c000.snappy.parquet
4 -rw-r--r-- 1 NBuser   12 Oct 18 02:02 .part-00002-b8a1ea0d-0637-4432-8ab6-8ec864edb6b0-c000.snappy.parquet.crc
4 -rw-r--r-- 1 NBuser  486 Oct 18 02:02 part-00003-ba20f466-8cb6-4827-9c10-218e8933f0f7-c000.snappy.parquet
4 -rw-r--r-- 1 NBuser   12 Oct 18 02:02 .part-00003-ba20f466-8cb6-4827-9c10-218e8933f0f7-c000.snappy.parquet.crc
```

### Scala Shell

1. Open a bash shell (if on windows use git bash, WSL, or any shell configured for bash commands)
2. Run a container from the built image with a bash entrypoint 

    ```bash
    docker run --rm -it --entrypoint bash delta_quickstart
    ```
3. Launch a scala interactive shell session

    ```bash
    $SPARK_HOME/bin/spark-shell --packages io.delta:${DELTA_PACKAGE_VERSION} \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    ```

4. 
    ```scala

        // Create Spark DataFrame
        val data = spark.range(0, 5)
        data.write.format("delta").save("/tmp/delta-table")

        // Read Delta table
        val df = spark.read.format("delta").load("/tmp/delta-table")                    

        // Show Delta table
        df.show()
        ```

5.  Follow the quickstart [here](https://docs.delta.io/latest/quick-start.html#create-a-table&language-scala)

To verify that you have a Delta table, you can list the contents within the folder of your Delta table. For example, in the previous code, you saved the table in /tmp/delta-table. Once you close your pyspark process, run a list command in your Docker shell and you should get something similar to below.

```bash
$ ls -lsgA /tmp/delta-table
total 36
4 drwxr-xr-x 2 NBuser 4096 Oct 18 02:02 _delta_log
4 -rw-r--r-- 1 NBuser  478 Oct 18 02:02 part-00000-b968d89a-b299-401f-a6db-ba0c160633ab-c000.snappy.parquet
4 -rw-r--r-- 1 NBuser   12 Oct 18 02:02 .part-00000-b968d89a-b299-401f-a6db-ba0c160633ab-c000.snappy.parquet.crc
4 -rw-r--r-- 1 NBuser  478 Oct 18 02:02 part-00001-f0f8ea27-b522-4c2c-8fe3-7224fccacb91-c000.snappy.parquet
4 -rw-r--r-- 1 NBuser   12 Oct 18 02:02 .part-00001-f0f8ea27-b522-4c2c-8fe3-7224fccacb91-c000.snappy.parquet.crc
4 -rw-r--r-- 1 NBuser  478 Oct 18 02:02 part-00002-b8a1ea0d-0637-4432-8ab6-8ec864edb6b0-c000.snappy.parquet
4 -rw-r--r-- 1 NBuser   12 Oct 18 02:02 .part-00002-b8a1ea0d-0637-4432-8ab6-8ec864edb6b0-c000.snappy.parquet.crc
4 -rw-r--r-- 1 NBuser  486 Oct 18 02:02 part-00003-ba20f466-8cb6-4827-9c10-218e8933f0f7-c000.snappy.parquet
4 -rw-r--r-- 1 NBuser   12 Oct 18 02:02 .part-00003-ba20f466-8cb6-4827-9c10-218e8933f0f7-c000.snappy.parquet.crc
```
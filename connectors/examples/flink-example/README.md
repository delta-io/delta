# Introduction
This is an example project that shows how to use `delta-flink` connector to read/write data from/to a Delta table using Apache Flink.

# Delta Source
Examples for Delta Flink source are using already created Delta table that can be found under
"src/main/resources/data/source_table_no_partitions" folder.
The detailed description of this table can be found in its [README.md](src/main/resources/data/source_table_no_partitions/README.md)

For Maven and SBT examples, if you wished to use Flink connector SNAPSHOT version,
you need to build it locally and publish to your local repository. You can do it using below code:
```shell
build/sbt standaloneCosmetic/publishM2
build/sbt flink/publishM2
```

### Local IDE
To run Flink example job reading data from Delta table from Local IDE
simply run class that contains `main` method from `org.example.source` package.

  For bounded mode:
  - `org.example.source.bounded.DeltaBoundedSourceExample` class.
  - `org.example.source.bounded.DeltaBoundedSourceUserColumnsExample` class.
  - `org.example.source.bounded.DeltaBoundedSourceVersionAsOfExample` class.

Examples for bound mode will terminate after reading all data from Snapshot. This is expected since those are examples of batch jobs.
The ConsoleSink out in logs can look something like log snipped below, where the order of log lines can be different for every run.
```
org.utils.ConsoleSink [] - Delta table row content: f1 -> [f1_val15], f2 -> [f2_val15], f3 -> [15]
org.utils.ConsoleSink [] - Delta table row content: f1 -> [f1_val6], f2 -> [f2_val6], f3 -> [6]
org.utils.ConsoleSink [] - Delta table row content: f1 -> [f1_val19], f2 -> [f2_val19], f3 -> [19]
```

For rest of the examples for bounded mode, you will see similar logs but with different number of rows (depending on version used for `versionAsOf` option)
or different number of columns depending on used value in builder's `.columnNames(String[])` method.

  For continuous mode:
  - `org.example.source.continuous.DeltaContinuousSourceExample` class.
  - `org.example.source.continuous.DeltaContinuousSourceStartingVersionExample` class.
  - `org.example.source.continuous.DeltaContinuousSourceUserColumnsExample` class.

Examples for continuous mode will not terminate by themselves. In order to stop, you need to terminate the manually using `ctr + c` command.
This is expected since those are examples of streaming jobs that by design run forever.
The ConsoleSink out in logs can look something like log snipped below, where the order of log lines can be different for every run.
```
org.utils.ConsoleSink [] - Delta table row content: f1 -> [f1_val6], f2 -> [f2_val6], f3 -> [6]
org.utils.ConsoleSink [] - Delta table row content: f1 -> [f1_val19], f2 -> [f2_val19], f3 -> [19]
org.utils.ConsoleSink [] - Delta table row content: f1 -> [f1_newVal_0], f2 -> [f2_newVal_0], f3 -> [0]
org.utils.ConsoleSink [] - Delta table row content: f1 -> [f1_newVal_1], f2 -> [f2_newVal_1], f3 -> [1]
```
The example is constructed in a way, after few moments from finishing reading Delta table consent, new records will begin to be added to the table.
The Sink connector will read them as well. New records will have `newVal` for `f1` and `f2` column values.

For rest of the examples for continuous mode, you will see similar logs but with different number of rows (depending on version used for `startingVersion` option)
or different number of columns depending on used value in builder's `.columnNames(String[])` method.

### Maven
To run Flink example job reading data from Delta table from Maven, simply run:
```shell
> cd examples/flink-example/
>
> mvn package exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass=org.example.source.bounded.DeltaBoundedSourceExample -Dstaging.repo.url={maven_repo} -Dconnectors.version={version}
```

In `-Dexec.mainClass` argument you can use any of the full class names from `Local IDE` paragraph.

### SBT
To run Flink example job reading data from Delta table from SBT, simply run:
```shell
> cd examples/
> export STANDALONE_VERSION=x.y.z  # update to desired version
> export EXTRA_MAVEN_REPO={staged_repo}  # include staged repo if desired
>
> build/sbt "flinkExample/runMain org.example.source.bounded.DeltaBoundedSourceExample"
```

Similar to `Maven` paragraph, here you can also use any of the full class names from the `Local IDE` paragraph as `build/sbt "flinkExample/runMain` argument.

# Delta Sink
## Run example for non-partitioned Delta table
To run example in-memory Flink job writing data a non-partitioned Delta table run:

### Local IDE
  Simply run `org.example.sink.DeltaSinkExample` class that contains `main` method

### Maven
```shell
> cd examples/flink-example/
>
> mvn package exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass=org.example.sink.DeltaSinkExample -Dstaging.repo.url={maven_repo} -Dconnectors.version={version}
```

### SBT
```shell
> cd examples/
> export STANDALONE_VERSION=x.y.z  # update to desired version
> export EXTRA_MAVEN_REPO={staged_repo}  # include staged repo if desired
>
> build/sbt "flinkExample/runMain org.example.sink.DeltaSinkExample"
```

## Run example for partitioned Delta table
To run example in-memory Flink job writing data a non-partitioned Delta table run:

### Local IDE
  Simply run `org.example.sink.DeltaSinkPartitionedTableExample` class that contains `main` method

### Maven
```shell
> cd examples/flink-example/
>
> mvn package exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass=org.example.sink.DeltaSinkPartitionedTableExample -Dstaging.repo.url={maven_repo} -Dconnectors.version={version}
```

### SBT
```shell
> cd examples/
> export STANDALONE_VERSION=x.y.z  # update to desired version
> export EXTRA_MAVEN_REPO={staged_repo}  # include staged repo if desired
>
> build/sbt "flinkExample/runMain org.example.sink.DeltaSinkPartitionedTableExample"
```

## Verify
After performing above steps you may observe your command line that will be printing descriptive information
about produced data. Streaming Flink job will run until manual termination and will be producing 1 event
in the interval of 800 millis by default.

To inspect written data look inside `examples/flink-example/src/main/resources/example_table` or
`examples/flink-example/src/main/resources/example_partitioned_table` which will contain created Delta tables along with the written Parquet files.

NOTE: there is no need to manually delete previous data to run the example job again - the example application will do it automatically

# Run an example on a local Flink cluster
## Setup 
1. Setup Flink cluster on your local machine by following the instructions provided [here](https://nightlies.apache.org/flink/flink-docs-release-1.13/try-flink/local_installation.html) (note: link redirects to Flink 1.13 release so be aware to choose your desired release).
2. Go to the examples directory in order to package the jar
```shell
> cd examples/flink-example/
> mvn -P cluster clean package -Dstaging.repo.url={maven_repo} -Dconnectors.version={version}
```
After that you should find the packaged fat-jar under path: `<connectors-repo-local-dir>/flink-example/target/flink-example-<version>-jar-with-dependencies.jar`
3. Assuming you've downloaded and extracted Flink binaries from step 1 to the directory `<local-flink-cluster-dir>` run:
```shell
> cd <local-flink-cluster-dir>
> ./bin/start-cluster.sh
> ./bin/flink run -c org.example.sink.DeltaSinkExampleCluster <connectors-repo-local-dir>/flink-example/target/flink-example-<version>-jar-with-dependencies.jar
```
The example above will submit Flink example job for Delta Sink. To submit FLink example job for Delta Source use
`org.example.source.bounded.DeltaBoundedSourceClusterExample` or `org.example.source.continuous.DeltaContinuousSourceClusterExample`.
First will submit a batch job, and second will submit streaming job.

NOTE:<br>
Before running cluster examples for Delta Source, you need to manually copy Delta table data from `src/main/resources/data/source_table_no_partitions`
to `/tmp/delta-flink-example/source_table`.

## Verify
### Dela Sink
Go the http://localhost:8081 on your browser where you should find Flink UI and you will be able to inspect your running job.
You can also look for the written files under `/tmp/delta-flink-example/<UUID>` directory.
![flink job ui](src/main/resources/assets/images/flink-cluster-job.png)

### Delta Source
Go the http://localhost:8081 on your browser where you should find Flink UI and you will be able to inspect your running job.
You can also look at Task Manager logs for `ConsoleSink` output.
![flink job ui](src/main/resources/assets/images/source-pipeline.png)
![flink job logs](src/main/resources/assets/images/source-pipeline-logs.png)

### Cleaning up
1. You cancel your job from the UI after you've verified your test. 
2. To shut down the cluster go back to the command line and run 
```shell
> ./bin/stop-cluster.sh
```

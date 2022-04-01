# Introduction
This is an example project that shows how to use `delta-flink` to write data to a Delta table using Apache Flink.

## Run example for non-partitioned Delta table
To run example in-memory Flink job writing data a non-partitioned Delta table run:

- with your local IDE:
  Simply run `io.delta.flink.example.sink.DeltaSinkExample` class that contains `main` method

- with Maven:
```
> cd examples/flink-example/
>
> mvn package exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass=example.DeltaSinkExample -Dstaging.repo.url={maven_repo} -Dconnectors.version={version}
```

- with SBT:
```
> cd examples/
> export STANDALONE_VERSION=0.4.0-SNAPSHOT  # update to desired version
> export EXTRA_MAVEN_REPO={staged_repo}  # include staged repo if desired
>
> build/sbt "flinkExample/runMain example.DeltaSinkExample"
```

## Run example for partitioned Delta table
To run example in-memory Flink job writing data a non-partitioned Delta table run:

- with your local IDE:
  Simply run `io.delta.flink.example.sink.DeltaSinkPartitionedTableExample` class that contains `main` method

- with Maven:
```
> cd examples/flink-example/
>
> mvn package exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass=example.DeltaSinkPartitionedTableExample -Dstaging.repo.url={maven_repo} -Dconnectors.version={version}
```

- with SBT:
```
> cd examples/
> export STANDALONE_VERSION=0.4.0-SNAPSHOT  # update to desired version
> export EXTRA_MAVEN_REPO={staged_repo}  # include staged repo if desired
>
> build/sbt "flinkExample/runMain example.DeltaSinkPartitionedTableExample"
```

## Verify
After performing above steps you may observe your command line that will be printing descriptive information
about produced data. Streaming Flink job will run until manual termination and will be producing 1 event
in the interval of 800 millis by default.

To inspect written data look inside `examples/flink-example/src/main/resources/example_table` or
`examples/flink-example/src/main/resources/example_partitioned_table` which will contain created Delta tables along with the written Parquet files.

NOTE: there is no need to manually delete previous data to run the example job again - the example application will do it automatically
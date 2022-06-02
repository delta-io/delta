# Introduction
This is an example project that shows how to use `delta-flink` to write data to a Delta table using Apache Flink.

## Run example for non-partitioned Delta table
To run example in-memory Flink job writing data a non-partitioned Delta table run:

- with your local IDE:
  Simply run `org.example.sink.DeltaSinkExample` class that contains `main` method

- with Maven:
```shell
> cd examples/flink-example/
>
> mvn package exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass=org.example.DeltaSinkExample -Dstaging.repo.url={maven_repo} -Dconnectors.version={version}
```

- with SBT:
```shell
> cd examples/
> export STANDALONE_VERSION=x.y.z  # update to desired version
> export EXTRA_MAVEN_REPO={staged_repo}  # include staged repo if desired
>
> build/sbt "flinkExample/runMain org.example.DeltaSinkExample"
```

## Run example for partitioned Delta table
To run example in-memory Flink job writing data a non-partitioned Delta table run:

- with your local IDE:
  Simply run `org.example.sink.DeltaSinkPartitionedTableExample` class that contains `main` method

- with Maven:
```shell
> cd examples/flink-example/
>
> mvn package exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass=org.example.DeltaSinkPartitionedTableExample -Dstaging.repo.url={maven_repo} -Dconnectors.version={version}
```

- with SBT:
```shell
> cd examples/
> export STANDALONE_VERSION=x.y.z  # update to desired version
> export EXTRA_MAVEN_REPO={staged_repo}  # include staged repo if desired
>
> build/sbt "flinkExample/runMain org.example.DeltaSinkPartitionedTableExample"
```

## Verify
After performing above steps you may observe your command line that will be printing descriptive information
about produced data. Streaming Flink job will run until manual termination and will be producing 1 event
in the interval of 800 millis by default.

To inspect written data look inside `examples/flink-example/src/main/resources/example_table` or
`examples/flink-example/src/main/resources/example_partitioned_table` which will contain created Delta tables along with the written Parquet files.

NOTE: there is no need to manually delete previous data to run the example job again - the example application will do it automatically

# Run example on a local Flink cluster
1. Setup Flink cluster on your local machine by following the instructions provided [here](https://nightlies.apache.org/flink/flink-docs-release-1.12/try-flink/local_installation.html) (note: link redirects to Flink 1.12 release so be aware to choose your desired release).
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
> ./bin/flink run -c org.example.DeltaSinkExample <connectors-repo-local-dir>/flink-example/target/flink-example-<version>-jar-with-dependencies.jar
```
4. Go the http://localhost:8081 on your browser where you should find Flink UI and you will be able to inspect your running job. You can also look for the written files under `/tmp/delta-flink-example/<UUID>` directory.
![flink job ui](src/main/resources/assets/images/flink-cluster-job.png)
5. You cancel your job from the UI after you've verified your test. 
6. To shut down the cluster go back to the command line and run 
```shell
> ./bin/stop-cluster.sh
```

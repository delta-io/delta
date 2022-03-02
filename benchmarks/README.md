# Benchmarks 

## Overview
This is a basic framework for writing benchmarks to measure Delta's performance. It is currently designed to run benchmark on Spark running in an EMR cluster. However, it can be easily extended for other Spark-based benchmarks. To get started, first download/clone this repository in your local machine. Then you have to set up an EMR cluster and run the benchmark scipts in this directory. See the next section for more details.

## Running benchmark

### Prerequisites
- An AWS account with necessary permissions to do the following:
  - Manage EMR clusters for running the benchmark
  - Read and write to an S3 bucket from the EMR cluster
- A S3 bucket which will be used to generate the TPC-DS data in different storage formats.
- A machine which has access to the AWS setup and where this repository has been downloaded or cloned. 

### Create EMR cluster
Create an EMR cluster that connects to the external Hive Metastore.  Here are the specifications of the EMR cluster required for running benchmarks.
- Master - i3.2xlarge
- Workers - i3.2xlarge
- No autoscaling, and default EBS storage.

Once the EMR cluster is ready, note the following: 
- Hostname of the EMR cluster master node.
- PEM file for SSH into the master node.
These will be needed to run the workloads in this framework. 

### Test the cluster setup
Navigate to your local copy of this repository and this benchmark directory. Then run the following steps.

#### Run simple test workload
Verify that you have the following information
  - <HOST_NAME>: EMR cluster master node host name
  - <PEM_FILE>: Local path to your PEM file for SSH into the master node.
  - <BENCHMARK_PATH>: S3 path where tables will be created. Make sure your AWS credentials have read/write permission to that path.
    
Then run a simple table write-read test: Run the following in your shell. 
    ```sh
    ./run-benchmark.py --cluster-hostname <HOSTNAME> -i <PEM_FILE> --benchmark-path <BENCHMARK_PATH> --benchmark test 
    ```

    If this works correctly, then you should see an output that look like this.
     
    ```text
    >>> Benchmark script generated and uploaded
    
    ...
    There is a screen on:
    12001..ip-172-31-21-247	(Detached)
    
    Files for this benchmark:
    20220126-191336-test-benchmarks.jar
    20220126-191336-test-cmd.sh
    20220126-191336-test-out.txt
    >>> Benchmark script started in a screen. Stdout piped into 20220126-191336-test-out.txt.Final report will be generated on completion in 20220126-191336-test-report.json.
    ```
   
    The test workload launched in a `screen` is going to run the following: 
    - Spark jobs to run a simple SQL query
    - Create a Delta table in the given location 
    - Read it back
    
    To see whether they worked correctly, SSH into the node and check the output of 20220126-191336-test-out.txt. Once the workload terminates, the last few lines should be something like the following:
    ```text
    RESULT:
    {
      "benchmarkSpecs" : {
        "benchmarkPath" : ...,
        "benchmarkId" : "20220126-191336-test"
      },
      "queryResults" : [ {
        "name" : "sql-test",
        "durationMs" : 11075
      }, {
        "name" : "db-list-test",
        "durationMs" : 208
      }, {
        "name" : "db-create-test",
        "durationMs" : 4070
      }, {
        "name" : "db-use-test",
        "durationMs" : 41
      }, {
        "name" : "table-drop-test",
        "durationMs" : 74
      }, {
        "name" : "table-create-test",
        "durationMs" : 33812
      }, {
        "name" : "table-query-test",
        "durationMs" : 4795
      } ]
    }
    FILE UPLOAD: Uploaded /home/hadoop/20220126-191336-test-report.json to s3:// ...
    SUCCESS
    ```
    The above metrics are also written to a json file and uploaded to the given path. Please verify that both the table and report are generated in that path. 


## Internals of the framework

Structure of this framework's code
- `build.sbt`, `project/`, `src/` form the SBT project which contains the Scala code that define the benchmark workload.
    - `Benchmark.scala` is the basic interface, and `TestBenchmark.scala` is a sample implementation.
- `run-benchmark.py` contains the specification of the benchmarks defined by name (e.g. `tpcds-3tb-delta`). Each benchmark specifications is defined by the following: 
    - Fully qualified name of the main Scala class to be started.
    - Command line argument for the main function.
    - Additional Maven artifact to load (example `io.delta:delta-core_2.12:1.0.0`).
    - Spark configurations to use.
- `scripts` has the core python scripts that are called by `run-benchmark.py`

The script `run-benchmark.py` does the following:
- Compile the Scala code into a uber jar.
- Upload it to the given hostname.
- Using ssh to the hostname, it will launch a screen and start the main class with spark-submit.

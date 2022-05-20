# Benchmarks 

## Overview
This is a basic framework for writing benchmarks to measure Delta's performance. It is currently designed to run benchmark on Spark running in an EMR or a Dataproc cluster. However, it can be easily extended for other Spark-based benchmarks. To get started, first download/clone this repository in your local machine. Then you have to set up a cluster and run the benchmark scripts in this directory. See the next section for more details.

## Running TPC-DS benchmark

This TPC-DS benchmark is constructed such that you have to run the following two steps. 
1. *Load data*: You have to create the TPC-DS database with all the Delta tables. To do that, the raw TPC-DS data has been provided as Apache Parquet files. In this step you will have to use your EMR or a Dataproc cluster to read the parquet files and rewrite them as Delta tables.
2. *Query data*: Then, using the tables definitions in the Hive Metatore, you can run the 99 benchmark queries.   

The next section will provide the detailed steps of how to setup the necessary Hive Metastore and a cluster, how to test the setup with small-scale data, and then finally run the full scale benchmark.

### Configure cluster with Amazon Web Services

#### Prerequisites
- An AWS account with necessary permissions to do the following:
  - Manage RDS instances for creating an external Hive Metastore
  - Manage EMR clusters for running the benchmark
  - Read and write to an S3 bucket from the EMR cluster
- A S3 bucket which will be used to generate the TPC-DS data.
- A machine which has access to the AWS setup and where this repository has been downloaded or cloned.

There are two ways to create infrastructure required for benchmarks - using provided [Terraform template](infrastructure/aws/terraform/README.md) or manually.

#### Create infrastructure manually

##### Create external Hive Metastore using Amazon RDS
Create an external Hive Metastore in a MySQL database using Amazon RDS with the following specifications:
- MySQL 8.x on a `db.m5.large`.
- General purpose SSDs, and no Autoscaling storage.
- Non-empty password for admin
- Same region, VPC, subnet as those you will run the EMR cluster. See AWS docs for more guidance.
  - *Note:* Region us-west-2 since that is what this benchmark has been most tested with.

After the database is ready, note the JDBC connection details, the username and password. We will need them for the next step. Note that this step needs to be done just once. All EMR clusters can connect and reused this Hive Metastsore. 
  
##### Create EMR cluster
Create an EMR cluster that connects to the external Hive Metastore.  Here are the specifications of the EMR cluster required for running benchmarks.
- EMR version 6.5.0 having Apache Spark 3.1
- Master - i3.2xlarge
- Workers - 16 x i3.2xlarge (or just 1 worker if you are just testing by running the 1GB benchmark).
- Hive-site configuration to connect to the Hive Metastore. See [Using an external MySQL database or Amazon Aurora](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hive-metastore-external.html) for more details.
- Same region, VPC, subnet as those of the Hive Metastore.
  - *Note:* Region us-west-2 since that is what this benchmark has been most tested with.
- No autoscaling, and default EBS storage.

Once the EMR cluster is ready, note the following: 
- Hostname of the EMR cluster master node.
- PEM file for SSH into the master node.
These will be needed to run the workloads in this framework. 

##### Prepare S3 bucket
Create a new S3 bucket (or use an existing one) which is in the same region as your EMR cluster.

_________________

### Configure cluster with Google Cloud Platform

#### Prerequisites
- A GCP account with necessary permissions to do the following:
  - Manage Dataproc clusters for running the benchmark
  - Manage Dataproc Metastore instances
  - Read and write to a GCS bucket from the Dataproc cluster
- A GCS bucket which will be used to generate the TPC-DS data.
- A machine which has access to the GCP setup and where this repository has been downloaded or cloned.
- SSH keys for a user which will be used to access the master node. The user's SSH key can be either [a project-wide key](https://cloud.google.com/compute/docs/connect/add-ssh-keys#add_ssh_keys_to_project_metadata) 
  or assigned to the [master node](https://cloud.google.com/compute/docs/connect/add-ssh-keys#after-vm-creation) only.
- Ideally, all GCP components used in benchmark should be in the same location (Storage bucket, Dataproc Metastore service and Dataproc cluster).

There are two ways to create infrastructure required for benchmarks - using provided [Terraform template](infrastructure/gcp/terraform/README.md) or manually.

#### Create infrastructure manually

##### Prepare GCS bucket
Create a new GCS bucket (or use an existing one) which is in the same region as your Dataproc cluster.

##### Create Dataproc Metastore
You can create [Dataproc metastore](https://cloud.google.com/dataproc-metastore/docs/create-service)
either via Web Console or gcloud command.

Sample create command:
```bash
gcloud metastore services create dataproc-metastore-for-benchmarks \
    --location=<region> \
    --tier=enterprise
```

##### Create Dataproc cluster
Here are the specifications of the Dataproc cluster required for running benchmarks.
- Image version >= 2.0 having Apache Spark 3.1
- Master - n2-highmem-8 (8 vCPU, 64 GB memory)
- Workers - 16 x n2-highmem-8 (or just 2 workers if you are just testing by running the 1GB benchmark).
- The cluster connects to the Dataproc Metastore.
- Same region and subnet as those of the Dataproc Metastore and GCS bucket.
- No autoscaling.

Sample create command:
```bash
gcloud dataproc clusters create delta-performance-benchmarks-cluster \
    --project <project-name> \
    --enable-component-gateway \
    --region <region> \
    --zone <zone> \
    --subnet default \
    --master-machine-type n2-highmem-8 \
    --master-boot-disk-type pd-ssd \
    --master-boot-disk-size 100 \
    --num-master-local-ssds 4 \
    --master-local-ssd-interface NVME \
    --num-workers 16 \
    --worker-machine-type n2-highmem-8 \
    --worker-boot-disk-type pd-ssd \
    --worker-boot-disk-size 100 \
    --num-worker-local-ssds 4 \
    --worker-local-ssd-interface NVME \
    --dataproc-metastore projects/<project-name>/locations/<region>/services/dataproc-metastore-for-benchmarks \
    --enable-component-gateway \
    --image-version 2.0-debian10
```

#### Input data
The benchmark is run using the raw TPC-DS data which has been provided as Apache Parquet files. There are two
predefined datasets of different size, 1GB and 3TB, located in `s3://devrel-delta-datasets/tpcds-2.13/tpcds_sf1_parquet/`
and `s3://devrel-delta-datasets/tpcds-2.13/tpcds_sf3000_parquet/`, respectively. Please keep in mind that
`devrel-delta-datasets` bucket is configured as [Requester Pays](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ObjectsinRequesterPaysBuckets.html) bucket,
so [access requests have to be configured properly](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ObjectsinRequesterPaysBuckets.html).

Unfortunately, Hadoop in versions available in Dataproc does not support *Requester Pays* feature. It will be available
as of Hadoop 3.3.4 ([HADOOP-14661](https://issues.apache.org/jira/browse/HADOOP-14661)).

In consequence, one need to copy the datasets to Google Storage manually before running benchmarks. The simplest
solution is to copy the data in two steps: first to a S3 bucket with *Requester Pays* disabled, then copy the data
using [Cloud Storage Transfer Service](https://cloud.google.com/storage-transfer/docs/how-to).

_________________

### Test the cluster setup
Navigate to your local copy of this repository and this benchmark directory. Then run the following steps.

#### Run simple test workload
Verify that you have the following information
  - <HOST_NAME>: Cluster master node host name
  - <PEM_FILE>: Local path to your PEM file for SSH into the master node.
  - <SSH_USER>: The username that will be used to SSH into the master node. The username is tied to the SSH key you
    have imported into the cloud. It defaults to `hadoop`.
  - <BENCHMARK_PATH>: Path where tables will be created. Make sure your credentials have read/write permission to that path.
  - <CLOUD_PROVIDER>: Currently either `gcp` or `aws`. For each storage type, different Delta properties might be added.
    
Then run a simple table write-read test: Run the following in your shell.

```sh
./run-benchmark.py \
    --cluster-hostname <HOSTNAME> \
    -i <PEM_FILE> \
    --ssh-user <SSH_USER> \
    --benchmark-path <BENCHMARK_PATH> \
    --cloud-provider <CLOUD_PROVIDER> \
    --benchmark test
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

#### Run 1GB TPC-DS
Now that you are familiar with how the framework runs the workload, you can try running the small scale TPC-DS benchmark.


1. Load data as Delta tables:
    ```bash
    ./run-benchmark.py \
        --cluster-hostname <HOSTNAME> \
        -i <PEM_FILE> \
        --ssh-user <SSH_USER> \
        --benchmark-path <BENCHMARK_PATH> \
        --cloud-provider <CLOUD_PROVIDER> \
        --benchmark tpcds-1gb-delta-load
    ```
   If you run the benchmark in GCP you should provide `--source-path <SOURCE_PATH>` parameter, where `<SOURCE_PATH>` is the location of the raw parquet input data files (see *Input data* section).
    ```bash
    ./run-benchmark.py \
        --cluster-hostname <HOSTNAME> \
        -i <PEM_FILE> \
        --ssh-user <SSH_USER> \
        --benchmark-path <BENCHMARK_PATH> \
        --source-path <SOURCE_PATH> \
        --cloud-provider gcp \
        --benchmark tpcds-1gb-delta-load
    ```

3. Run queries on Delta tables:
    ```bash
    ./run-benchmark.py \
        --cluster-hostname <HOSTNAME> \
        -i <PEM_FILE> \
        --ssh-user <SSH_USER> \
        --benchmark-path <BENCHMARK_PATH> \
        --cloud-provider <CLOUD_PROVIDER> \
        --benchmark tpcds-1gb-delta
    ```

### Run 3TB TPC-DS
Finally, you are all set up to run the full scale benchmark. Similar to the 1GB benchmark, run the following

1. Load data as Delta tables:
    ```bash
    ./run-benchmark.py \
        --cluster-hostname <HOSTNAME> \
        -i <PEM_FILE> \
        --ssh-user <SSH_USER> \
        --benchmark-path <BENCHMARK_PATH> \
        --cloud-provider <CLOUD_PROVIDER> \
        --benchmark tpcds-3tb-delta-load
    ```
   If you run the benchmark in GCP you should provide `--source-path <SOURCE_PATH>` parameter, where `<SOURCE_PATH>` is the location of the raw parquet input data files (see *Input data* section).
    ```bash
    ./run-benchmark.py \
        --cluster-hostname <HOSTNAME> \
        -i <PEM_FILE> \
        --ssh-user <SSH_USER> \
        --benchmark-path <BENCHMARK_PATH> \
        --source-path <SOURCE_PATH> \
        --cloud-provider gcp \
        --benchmark tpcds-3tb-delta-load
    ```

2. Run queries on Delta tables:
    ```bash
    ./run-benchmark.py \
        --cluster-hostname <HOSTNAME> \
        -i <PEM_FILE> \
        --ssh-user <SSH_USER> \
        --benchmark-path <BENCHMARK_PATH> \
        --cloud-provider <CLOUD_PROVIDER> \
        --benchmark tpcds-3tb-delta
    ```

Compare the results using the generated JSON files.

_________________

## Internals of the framework

Structure of this framework's code
- `build.sbt`, `project/`, `src/` form the SBT project which contains the Scala code that define the benchmark workload.
    - `Benchmark.scala` is the basic interface, and `TestBenchmark.scala` is a sample implementation.
- `run-benchmark.py` contains the specification of the benchmarks defined by name (e.g. `tpcds-3tb-delta`). Each benchmark specification is defined by the following: 
    - Fully qualified name of the main Scala class to be started.
    - Command line argument for the main function.
    - Additional Maven artifact to load (example `io.delta:delta-core_2.12:1.0.0`).
    - Spark configurations to use.
- `scripts` has the core python scripts that are called by `run-benchmark.py`

The script `run-benchmark.py` does the following:
- Compile the Scala code into a uber jar.
- Upload it to the given hostname.
- Using ssh to the hostname, it will launch a screen and start the main class with spark-submit.

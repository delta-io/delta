# AWS benchmarks

The document describes step by step how to run Delta Lake benchmarks on AWS infrastructure.
Currently, you can run benchmarks on either EMR on EKS.

## Prerequisites

There are a few steps that are common for both deployment options.

1. Install [Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli?in=terraform/aws-get-started).
2. Create an IAM user which will be used to create benchmarks infrastructure. Ensure that your AWS CLI is configured.
   You should either have valid credentials in shared credentials file (e.g. `~/.aws/credentials`)
   ```
   [default]
   aws_access_key_id = anaccesskey
   aws_secret_access_key = asecretkey
   ```
   or export keys as environment variables:
   ```bash
   export AWS_ACCESS_KEY_ID="anaccesskey"
   export AWS_SECRET_ACCESS_KEY="asecretkey"
   ```

## Create EMR infrastructure with Terraform

1. Add permissions for the IAM user. You can either assign `AdministratorAccess` AWS managed policy (discouraged)
   or assign AWS managed policies in a more granular way:
    * `IAMFullAccess`
    * `AmazonVPCFullAccess`
    * `AmazonEMRFullAccessPolicy_v2`
    * `AmazonElasticMapReduceFullAccess`
    * `AmazonRDSFullAccess`
    * `AmazonS3FullAccess`
    * a custom policy for EC2 key pairs management
      ```json
      {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "ec2:ImportKeyPair",
              "ec2:CreateKeyPair",
              "ec2:DeleteKeyPair"
            ],
            "Resource": "arn:aws:ec2:*:*:key-pair/benchmarks_key_pair"
          }
        ]
      }
      ```

2. Create Terraform variable file `benchmarks/infrastructure/aws/terraform/terraform.tfvars` and fill in variable values.
   ```tf
   region                 = "<REGION>"
   availability_zone1     = "<AVAILABILITY_ZONE1>"
   availability_zone2     = "<AVAILABILITY_ZONE2>"
   benchmarks_bucket_name = "<BUCKET_NAME>"
   source_bucket_name     = "<SOURCE_BUCKET_NAME>"
   mysql_user             = "<MYSQL_USER>"
   mysql_password         = "<MYSQL_PASSWORD>"
   emr_public_key_path    = "<EMR_PUBLIC_KEY_PATH>"
   user_ip_address        = "<MY_IP>"
   emr_workers            = WORKERS_COUNT
   tags                   = {
     key1 = "value1"
     key2 = "value2"
   }
   ```
   Please check `variables.tf` to learn more about each parameter.

3. Run:
   ```bash
   terraform init
   terraform validate
   terraform apply -auto-approve \
       -target=module.networking \
       -target=module.storage \
       -target=module.metastore-rds \
       -target=module.processing-emr
   ```
   As a result, a new VPC, a S3 bucket, a MySQL instance (metastore) and a EMR cluster will be created.
   The `apply` command returns `master_node_address` that will be used when running benchmarks.
   ```
   Apply complete! Resources: 16 added, 0 changed, 0 destroyed.
   Outputs:
   master_node_address = "35.165.163.250"
   ```

4. Once the benchmarks are finished, destroy the resources.
   ```bash
   terraform destroy
   ```
   If the S3 bucket contains any objects, it will not be destroyed automatically.
   One need to do that manually to avoid any accidental data loss.
   ```
   Error: deleting S3 Bucket (my-bucket): BucketNotEmpty: The bucket you tried to delete is not empty 
   status code: 409, request id: Q11TYZ5E0B23QGQ2, host id: WdeFY88km5IBhy+bi2hqXzgjBxjrn1+OPtCstsWDjkwGNCyEhXYjq330DZq1jbfNXojBEejH6Wg=
   ```

## Create EKS infrastructure with Terraform

1. Prepare input data.
   By default, the source data is fetched from `devrel-delta-datasets` bucket, which is configured as
   [Requester Pays](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ObjectsinRequesterPaysBuckets.html) bucket,
   so [access requests have to be configured properly](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ObjectsinRequesterPaysBuckets.html).

   Unfortunately, Hadoop in versions compatible with Spark does not support *Requester Pays* feature yet.
   It will be available as of Hadoop 3.3.9 ([HADOOP-14661](https://issues.apache.org/jira/browse/HADOOP-14661)).
   In consequence, if your account does not belong to an organization owning the source bucket,
   you need to copy the datasets to a S3 bucket which is not configured as "requester pays" bucket.

   There are two predefined datasets of different size, 1GB and 3TB, located in `s3://devrel-delta-datasets/tpcds-2.13/tpcds_sf1_parquet/`
   and `s3://devrel-delta-datasets/tpcds-2.13/tpcds_sf3000_parquet/`, respectively.

2. Install [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl).

3. Add permissions for the IAM user. You can either assign `AdministratorAccess` AWS managed policy (discouraged)
   or assign AWS managed policies in a more granular way:
    * `IAMFullAccess`
    * `AmazonVPCFullAccess`
    * `AmazonRDSFullAccess`
    * `AmazonS3FullAccess`
    * `AmazonEC2FullAccess`
    * `AmazonEC2ContainerRegistryFullAccess`
    * Allow all `eks:*` actions.

4. Create Terraform variable file `benchmarks/infrastructure/aws/terraform/terraform.tfvars` and fill in variable values.
   ```tf
   region                 = "<REGION>"
   availability_zone1     = "<AVAILABILITY_ZONE1>"
   availability_zone2     = "<AVAILABILITY_ZONE2>"
   benchmarks_bucket_name = "<BUCKET_NAME>"
   source_bucket_name     = "<SOURCE_BUCKET_NAME>"
   mysql_user             = "<MYSQL_USER>"
   mysql_password         = "<MYSQL_PASSWORD>"
   eks_workers            = WORKERS_COUNT
   tags                   = {
     key1 = "value1"
     key2 = "value2"
   }
   ```
   Please check `variables.tf` to learn more about each parameter.

5. Run:
   ```bash
   terraform init
   terraform validate
   terraform apply -auto-approve \
       -target=module.networking \
       -target=module.ecr \
       -target=module.storage \
       -target=module.metastore-rds \
       -target=module.processing-eks
   ```
   As a result, a new VPC, a S3 bucket, a MySQL instance (metastore), ECR registry and an EKS cluster will be created.
   The `apply` command returns `eks_cluster_endpoint` and `metastore_endpoint` (and others)
   that will be used when running benchmarks.
   ```
   Apply complete! Resources: 23 added, 0 changed, 0 destroyed.
   Outputs:
   account_id = "<account_id>"
   eks_cluster_endpoint = "https://19C4690AF99E49798A81086E6168F765.gr7.us-west-2.eks.amazonaws.com"
   metastore_endpoint = "terraform-20220803140719966600000001.cjutoqgei0vo.us-west-2.rds.amazonaws.com:3306"
   ```

6. Update kubeconfig:
   ```bash
   aws eks --region us-west-2 update-kubeconfig --name benchmarks-eks-cluster
   ```

7. Check if you can connect to the cluster by running some `kubectl` commands, for instance:
   ```bash
   $ kubectl get namespaces
   NAME              STATUS   AGE
   default           Active   7m12s
   kube-node-lease   Active   7m14s
   kube-public       Active   7m14s
   kube-system       Active   7m14s
   ```

8. Create Spark docker image and create kubernetes infrastructure:
   ```bash
   ./build-docker-and-kubernetes.sh
   ```
   Script output contains newly created docker image URI:
   ```
   ...
   namespace/benchmarks created
   serviceaccount/benchmarks-sa created
   role.rbac.authorization.k8s.io/benchmarks created
   rolebinding.rbac.authorization.k8s.io/benchmarks-role-binding created
   configmap/hive-site created
   pod/benchmarks-edge-node created
   Spark docker image URI: 781336771001.dkr.ecr.us-west-2.amazonaws.com/delta-benchmarks-spark:0.1
   ```
   By default `apache/spark:v3.2.1` image is used. You can specify a different version by adding `--spark-version` parameter.
   ```bash
   ./build-docker-and-kubernetes.sh --spark-version v3.2.1
   ```

9. Run benchmarks in a similar way as described in the main instruction. However, keep in mind that some
   `run-benchmark.py` parameter differs. Sample run commands:

   ```bash
   python3 run-benchmark.py \
       --k8s-cluster-endpoint <CLUSTER_ENDPOINT> \
       --docker-image <DOCKER_IMAGE> \
       --cloud-provider <CLOUD_PROVIDER> \
       --benchmark-path <BENCHMARKS_PATH> \
       --benchmark test \
       --spark-conf spark.executor.instances=8
   ```
   Required parameters:
   - <CLUSTER_ENDPOINT>: Endpoint URL of the Kubernetes cluster created in step 5.
   - <DOCKER_IMAGE>: Docker image URI, which was created in step 8.
     have imported into the cloud. It defaults to `hadoop`.
   - <CLOUD_PROVIDER>: Currently either `gcp` or `aws`. For each storage type, different Delta properties might be added.
   - <BENCHMARK_PATH>: Path where tables will be created. Make sure your credentials have read/write permission to that path.

   In addition, you need to explicitly define the number of spark executor instances. Since the EKS cluster
   consists of `i3.2xlarge` nodes, each of which has 8 cores, you should set `spark.executor.instances` to `N*8` where
   `N` is the `eks_workers` specified in `terraform.tfvars`.

10. Once the benchmarks are finished, destroy the resources.
    ```bash
    terraform destroy
    ```
    If the S3 bucket contains any objects, it will not be destroyed automatically.
    One need to do that manually to avoid any accidental data loss.
    ```
    Error: deleting S3 Bucket (my-bucket): BucketNotEmpty: The bucket you tried to delete is not empty 
    status code: 409, request id: Q11TYZ5E0B23QGQ2, host id: WdeFY88km5IBhy+bi2hqXzgjBxjrn1+OPtCstsWDjkwGNCyEhXYjq330DZq1jbfNXojBEejH6Wg=
    ```

### Known issues

#### Spark --packages parameter does not work for Kubernetes in cluster deployment

Related JIRA issue: [SPARK-32545](https://issues.apache.org/jira/browse/SPARK-32545). To avoid the issue, one need to run spark jobs in client deployment mode.
Alternatively, as a workaround, maven dependencies should be included into the fat-jar.

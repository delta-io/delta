# Create infrastructure with Terraform

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
3. Add permissions for the IAM user. You can either assign `AdministratorAccess` AWS managed policy (discouraged)
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

4. Create Terraform variable file `benchmarks/infrastructure/aws/terraform/terraform.tfvars` and fill in variable values.
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

5. Run:
   ```bash
   terraform init
   terraform validate
   terraform apply
   ```
   As a result, a new VPC, a S3 bucket, a MySQL instance (metastore) and a EMR cluster will be created.
   The `apply` command returns `master_node_address` that will be used when running benchmarks.
   ```
   Apply complete! Resources: 16 added, 0 changed, 0 destroyed.
   Outputs:
   master_node_address = "35.165.163.250"
   ```

6. Once the benchmarks are finished, destroy the resources.
   ```bash
   terraform destroy
   ```
   If the S3 bucket contains any objects, it will not be destroyed automatically.
   One need to do that manually to avoid any accidental data loss.
   ```
   Error: deleting S3 Bucket (my-bucket): BucketNotEmpty: The bucket you tried to delete is not empty 
   status code: 409, request id: Q11TYZ5E0B23QGQ2, host id: WdeFY88km5IBhy+bi2hqXzgjBxjrn1+OPtCstsWDjkwGNCyEhXYjq330DZq1jbfNXojBEejH6Wg=
   ```

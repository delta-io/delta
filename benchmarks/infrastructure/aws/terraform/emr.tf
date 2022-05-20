resource "aws_emr_cluster" "benchmarks" {
  name                              = "delta_performance_benchmarks_cluster"
  release_label                     = "emr-6.5.0"
  applications                      = ["Spark", "Hive"]
  termination_protection            = false
  keep_job_flow_alive_when_no_steps = true
  ec2_attributes {
    instance_profile                  = aws_iam_instance_profile.benchmarks_emr_profile.arn
    key_name                          = aws_key_pair.benchmarks.key_name
    subnet_id                         = aws_subnet.benchmarks_subnet1.id
    emr_managed_master_security_group = aws_security_group.emr.id
    emr_managed_slave_security_group  = aws_security_group.emr.id
  }
  master_instance_group {
    instance_type = "i3.2xlarge"
  }
  core_instance_group {
    instance_type  = "i3.2xlarge"
    instance_count = var.emr_workers
  }

  configurations_json = <<EOF
  [
    {
      "Classification": "hive-site",
      "Properties": {
        "javax.jdo.option.ConnectionURL": "jdbc:mysql://${aws_db_instance.benchmarks_metastore_service.endpoint}/hive?createDatabaseIfNotExist=true",
        "javax.jdo.option.ConnectionDriverName": "org.mariadb.jdbc.Driver",
        "javax.jdo.option.ConnectionUserName": "${var.mysql_user}",
        "javax.jdo.option.ConnectionPassword": "${var.mysql_password}"
      }
    }
  ]
EOF
  service_role        = aws_iam_role.benchmarks_iam_emr_service_role.arn
  depends_on          = [
    aws_db_instance.benchmarks_metastore_service,
    aws_s3_bucket.benchmarks_data
  ]
}

resource "aws_security_group" "emr" {
  name   = "benchmarks_master_security_group"
  vpc_id = aws_vpc.this.id
  ingress {
    description = "Allow inbound traffic from given IP."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["${var.user_ip_address}/32"]
  }
  egress {
    description      = "Allow all outbound traffic."
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
  # Amazon EMR will automatically add rules enabling traffic between all nodes.
}

# According to: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-roles-custom.html
#   To customize permissions, we recommend that you create new roles and policies. Begin with the permissions in
#   the managed policies for the default roles. Then, copy and paste the contents to new policy statements, modify
#   the permissions as appropriate, and attach the modified permissions policies to the roles that you create.
resource "aws_iam_role" "benchmarks_iam_emr_service_role" {
  name               = "iam_emr_service_role"
  assume_role_policy = <<EOF
{
  "Version": "2008-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "elasticmapreduce.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "benchmarks_iam_emr_service_policy" {
  name = "iam_emr_service_policy"
  role = aws_iam_role.benchmarks_iam_emr_service_role.id

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::${var.benchmarks_bucket_name}",
                "arn:aws:s3:::${var.source_bucket_name}"
            ],
            "Action": [
                "s3:ListBucket"
            ]
        },
        {
            "Effect": "Allow",
            "Resource": "arn:aws:s3:::${var.benchmarks_bucket_name}/*",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ]
        },
        {
            "Effect": "Allow",
            "Resource": "arn:aws:s3:::${var.source_bucket_name}/*",
            "Action": [
                "s3:GetObject"
            ]
        },
        {
            "Effect": "Allow",
            "Resource": "*",
            "Action": [
                "ec2:AuthorizeSecurityGroupEgress",
                "ec2:AuthorizeSecurityGroupIngress",
                "ec2:CancelSpotInstanceRequests",
                "ec2:CreateNetworkInterface",
                "ec2:CreateSecurityGroup",
                "ec2:CreateTags",
                "ec2:DeleteNetworkInterface",
                "ec2:DeleteSecurityGroup",
                "ec2:DeleteTags",
                "ec2:DescribeAvailabilityZones",
                "ec2:DescribeAccountAttributes",
                "ec2:DescribeDhcpOptions",
                "ec2:DescribeInstanceStatus",
                "ec2:DescribeInstances",
                "ec2:DescribeKeyPairs",
                "ec2:DescribeNetworkAcls",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DescribePrefixLists",
                "ec2:DescribeRouteTables",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeSpotInstanceRequests",
                "ec2:DescribeSpotPriceHistory",
                "ec2:DescribeSubnets",
                "ec2:DescribeVpcAttribute",
                "ec2:DescribeVpcEndpoints",
                "ec2:DescribeVpcEndpointServices",
                "ec2:DescribeVpcs",
                "ec2:DetachNetworkInterface",
                "ec2:ModifyImageAttribute",
                "ec2:ModifyInstanceAttribute",
                "ec2:RequestSpotInstances",
                "ec2:RevokeSecurityGroupEgress",
                "ec2:RunInstances",
                "ec2:TerminateInstances",
                "ec2:DeleteVolume",
                "ec2:DescribeVolumeStatus",
                "ec2:DescribeVolumes",
                "ec2:DetachVolume",
                "iam:GetRole",
                "iam:GetRolePolicy",
                "iam:ListInstanceProfiles",
                "iam:ListRolePolicies",
                "iam:PassRole"
            ]
        }
    ]
}
EOF
}

resource "aws_iam_role" "benchmarks_iam_emr_profile_role" {
  name = "iam_emr_profile_role"

  assume_role_policy = <<EOF
{
  "Version": "2008-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_instance_profile" "benchmarks_emr_profile" {
  name = "emr_profile"
  role = aws_iam_role.benchmarks_iam_emr_profile_role.name
}

resource "aws_iam_role_policy" "benchmarks_iam_emr_profile_policy" {
  name = "iam_emr_profile_policy"
  role = aws_iam_role.benchmarks_iam_emr_profile_role.id

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::${var.benchmarks_bucket_name}",
                "arn:aws:s3:::${var.source_bucket_name}"
            ],
            "Action": [
                "s3:ListBucket"
            ]
        },
        {
            "Effect": "Allow",
            "Resource": "arn:aws:s3:::${var.benchmarks_bucket_name}/*",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ]
        },
        {
            "Effect": "Allow",
            "Resource": "arn:aws:s3:::${var.source_bucket_name}/*",
            "Action": [
                "s3:GetObject"
            ]
        },
        {
            "Effect": "Allow",
            "Resource": "*",
            "Action": [
                "ec2:Describe*",
                "elasticmapreduce:Describe*",
                "elasticmapreduce:ListBootstrapActions",
                "elasticmapreduce:ListClusters",
                "elasticmapreduce:ListInstanceGroups",
                "elasticmapreduce:ListInstances",
                "elasticmapreduce:ListSteps"
            ]
        }
    ]
}
EOF
}

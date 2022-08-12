resource "aws_db_instance" "metastore_service" {
  engine                 = "mysql"
  engine_version         = "8.0.28"
  instance_class         = "db.m5.large"
  db_name                = "hive"
  username               = var.mysql_user
  password               = var.mysql_password
  availability_zone      = var.availability_zone1
  skip_final_snapshot    = true
  allocated_storage      = 50
  parameter_group_name   = aws_db_parameter_group.metastore_service.name
  db_subnet_group_name   = aws_db_subnet_group.metastore_service.name
  vpc_security_group_ids = [aws_security_group.metastore_service.id]
}

resource "aws_db_subnet_group" "metastore_service" {
  name       = "benchmarks_subnet_group_for_metastore_${var.benchmark_run_id}"
  subnet_ids = [var.subnet1_id, var.subnet2_id]
}

resource "aws_db_parameter_group" "metastore_service" {
  name   = "benchmarks-metastore-service-pg-${var.benchmark_run_id}"
  family = "mysql8.0"
  # character set and collation has to be set to latin1 in order to avoid error when auto-creating schema.
  #   java.sql.SQLSyntaxErrorException: (conn=39) Column length too big for column 'PARAM_VALUE' (max = 16383); use BLOB or TEXT instead
  # https://docs.microsoft.com/en-us/azure/databricks/kb/metastore/create-table-error-external-hive
  parameter {
    name  = "character_set_server"
    value = "latin1"
  }
  parameter {
    name  = "collation_server"
    value = "latin1_bin"
  }
}

resource "aws_security_group" "metastore_service" {
  name   = "benchmarks_metastore_security_group_${var.benchmark_run_id}"
  vpc_id = var.vpc_id
  ingress {
    from_port   = 3306
    to_port     = 3306
    protocol    = "TCP"
    cidr_blocks = ["10.0.0.0/16"]
  }
  egress {
    description = "Allow all outbound traffic."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

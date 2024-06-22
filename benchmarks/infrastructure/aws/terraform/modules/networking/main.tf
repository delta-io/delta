resource "aws_vpc" "this" {
  cidr_block = "10.0.0.0/16"
}

resource "aws_subnet" "benchmarks_subnet1" {
  vpc_id            = aws_vpc.this.id
  availability_zone = var.availability_zone1
  cidr_block        = "10.0.0.0/17"
}

# There are two subnets needed to create an RDS subnet group. In fact this one is unused.
# If DB subnet group is built using only one AZ, the following error is thrown:
#     The DB subnet group doesn't meet Availability Zone (AZ) coverage requirement.
#     Current AZ coverage: us-west-2a. Add subnets to cover at least 2 AZs.
resource "aws_subnet" "benchmarks_subnet2" {
  vpc_id            = aws_vpc.this.id
  availability_zone = var.availability_zone2
  cidr_block        = "10.0.128.0/17"
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
}

resource "aws_default_route_table" "public" {
  default_route_table_id = aws_vpc.this.default_route_table_id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }
}

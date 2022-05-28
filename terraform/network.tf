resource "aws_vpc" "vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
}

resource "aws_subnet" "subnet_public_emr" {
  vpc_id     = aws_vpc.vpc.id
  cidr_block = "10.0.1.0/24"
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.vpc.id
}

resource "aws_route_table" "rt" {
  vpc_id = aws_vpc.vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }
}

resource "aws_main_route_table_association" "rta" {
  vpc_id         = aws_vpc.vpc.id
  route_table_id = aws_route_table.rt.id
}

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region = "us-east-1"
}

resource "aws_instance" "openwindenergy_server" {
  ami           = "ami-0c4e709339fa8521a"
  instance_type = "t4g.xlarge"
  security_groups = ["${aws_security_group.ingress_all_test.id}"]
  subnet_id = "${aws_subnet.subnet_uno.id}"
  user_data = <<EOF
#!/bin/bash
echo "SERVER_USERNAME=${var.adminname}
SERVER_PASSWORD=${var.password}" >> /tmp/.env
sudo apt update -y
sudo apt install wget -y
wget https://raw.githubusercontent.com/open-wind/openwindenergy/refs/heads/main/openwindenergy-build-ubuntu.sh
chmod +x openwindenergy-build-ubuntu.sh
sudo ./openwindenergy-build-ubuntu.sh
EOF

  tags = {
    Name = "openwindenergy-server"
  }

  root_block_device {
    volume_size = 120
    volume_type = "gp3"
    encrypted   = false
  }
}

resource "aws_vpc" "openwindenergy_env" {
  cidr_block = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support = true
  tags = {
    Name = "openwindenergy_env"
  }
}

resource "aws_eip" "ip_openwindenergy_env" {
  instance = "${aws_instance.openwindenergy_server.id}"
  vpc      = true
}

resource "aws_internet_gateway" "openwindenergy_env_gw" {
  vpc_id = "${aws_vpc.openwindenergy_env.id}"
  tags = {
    Name = "openwindenergy_env_gw"
  }
}

resource "aws_subnet" "subnet_uno" {
  cidr_block = "${cidrsubnet(aws_vpc.openwindenergy_env.cidr_block, 3, 1)}"
  vpc_id = "${aws_vpc.openwindenergy_env.id}"
  availability_zone = "us-east-1a"
}

resource "aws_route_table" "route_table_openwindenergy_env" {
  vpc_id = "${aws_vpc.openwindenergy_env.id}"
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = "${aws_internet_gateway.openwindenergy_env_gw.id}"
  }
  tags = {
    Name = "openwindenergy_env_route_table"
  }
}
resource "aws_route_table_association" "subnet_association" {
  subnet_id      = "${aws_subnet.subnet_uno.id}"
  route_table_id = "${aws_route_table.route_table_openwindenergy_env.id}"
}

resource "aws_security_group" "ingress_all_test" {
  name = "allow_all_sg"
  description = "Allow SSH, HTTP and HTTPS"
  vpc_id = "${aws_vpc.openwindenergy_env.id}"
  ingress {
    cidr_blocks = [
      "0.0.0.0/0"
    ]
    from_port = 22
    to_port = 22
    protocol = "tcp"
  }

  ingress {
    cidr_blocks = [
      "0.0.0.0/0"
    ]
    from_port = 80
    to_port = 80
    protocol = "tcp"
  }

  ingress {
    cidr_blocks = [
      "0.0.0.0/0"
    ]
    from_port = 443
    to_port = 443
    protocol = "tcp"
  }

  // Terraform removes the default rule
  egress {
   from_port = 0
   to_port = 0
   protocol = "-1"
   cidr_blocks = ["0.0.0.0/0"]
 }
}

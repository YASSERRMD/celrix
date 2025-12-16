variable "cluster_name" {
  default = "celrix"
}

variable "node_count" {
  default = 3
}

variable "vpc_id" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

resource "aws_security_group" "celrix" {
  name   = "${var.cluster_name}-sg"
  vpc_id = var.vpc_id

  ingress {
    from_port   = 6380
    to_port     = 6380
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_ecs_cluster" "celrix" {
  name = var.cluster_name
}

output "cluster_name" {
  value = aws_ecs_cluster.celrix.name
}

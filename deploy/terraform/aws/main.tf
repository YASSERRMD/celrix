# CELRIX Terraform Module - AWS
# Deploy CELRIX cluster on AWS ECS/EKS

variable "cluster_name" {
  description = "Name of the CELRIX cluster"
  type        = string
  default     = "celrix"
}

variable "node_count" {
  description = "Number of CELRIX nodes"
  type        = number
  default     = 3
}

variable "instance_type" {
  description = "EC2 instance type"
  type        = string
  default     = "r6g.large"
}

variable "vpc_id" {
  description = "VPC ID for deployment"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for deployment"
  type        = list(string)
}

variable "enable_tls" {
  description = "Enable TLS encryption"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}

# Security Group
resource "aws_security_group" "celrix" {
  name        = "${var.cluster_name}-sg"
  description = "Security group for CELRIX cluster"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 6380
    to_port     = 6380
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
    description = "CELRIX client port"
  }

  ingress {
    from_port   = 16380
    to_port     = 16380
    protocol    = "tcp"
    self        = true
    description = "CELRIX cluster bus"
  }

  ingress {
    from_port   = 9090
    to_port     = 9090
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
    description = "Admin API"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.cluster_name}-sg"
  })
}

# ECS Cluster
resource "aws_ecs_cluster" "celrix" {
  name = var.cluster_name

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = var.tags
}

# ECS Task Definition
resource "aws_ecs_task_definition" "celrix" {
  family                   = var.cluster_name
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = 2048
  memory                   = 4096
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name  = "celrix"
      image = "celrix/celrix:latest"
      
      portMappings = [
        {
          containerPort = 6380
          hostPort      = 6380
          protocol      = "tcp"
        },
        {
          containerPort = 16380
          hostPort      = 16380
          protocol      = "tcp"
        },
        {
          containerPort = 9090
          hostPort      = 9090
          protocol      = "tcp"
        }
      ]

      environment = [
        {
          name  = "CELRIX_CLUSTER_ENABLED"
          value = "true"
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = "/ecs/${var.cluster_name}"
          "awslogs-region"        = data.aws_region.current.name
          "awslogs-stream-prefix" = "celrix"
        }
      }

      healthCheck = {
        command     = ["CMD-SHELL", "curl -f http://localhost:9090/health || exit 1"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 60
      }
    }
  ])

  tags = var.tags
}

# ECS Service
resource "aws_ecs_service" "celrix" {
  name            = var.cluster_name
  cluster         = aws_ecs_cluster.celrix.id
  task_definition = aws_ecs_task_definition.celrix.arn
  desired_count   = var.node_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = [aws_security_group.celrix.id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn = aws_service_discovery_service.celrix.arn
  }

  tags = var.tags
}

# Service Discovery
resource "aws_service_discovery_private_dns_namespace" "celrix" {
  name = "${var.cluster_name}.local"
  vpc  = var.vpc_id
}

resource "aws_service_discovery_service" "celrix" {
  name = var.cluster_name

  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.celrix.id

    dns_records {
      ttl  = 10
      type = "A"
    }

    routing_policy = "MULTIVALUE"
  }

  health_check_custom_config {
    failure_threshold = 1
  }
}

# IAM Roles
resource "aws_iam_role" "ecs_execution" {
  name = "${var.cluster_name}-ecs-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role" "ecs_task" {
  name = "${var.cluster_name}-ecs-task"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "celrix" {
  name              = "/ecs/${var.cluster_name}"
  retention_in_days = 30

  tags = var.tags
}

# Data sources
data "aws_region" "current" {}

# Outputs
output "cluster_endpoint" {
  description = "CELRIX cluster endpoint"
  value       = "${var.cluster_name}.${aws_service_discovery_private_dns_namespace.celrix.name}:6380"
}

output "admin_endpoint" {
  description = "Admin API endpoint"
  value       = "${var.cluster_name}.${aws_service_discovery_private_dns_namespace.celrix.name}:9090"
}

output "security_group_id" {
  description = "Security group ID"
  value       = aws_security_group.celrix.id
}

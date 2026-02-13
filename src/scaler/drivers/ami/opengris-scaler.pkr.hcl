packer {
  required_plugins {
    amazon = {
      version = "~> 1"
      source  = "github.com/hashicorp/amazon"
    }
  }
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "version" {
  type    = string
}

variable "ami_regions" {
  type    = list(string)
  default = []
  description = "A list of regions to copy the AMI to."
}

variable "ami_groups" {
  type    = list(string)
  default = ["all"]
  description = "A list of groups to share the AMI with. Set to ['all'] to make public."
}

variable "python_version" {
  type    = string
  default = "3.13"
}

source "amazon-ebs" "opengris-scaler" {
  ami_name      = "opengris-scaler-${var.version}-py${var.python_version}"
  instance_type = "t2.small"
  region        = var.aws_region
  ami_regions   = var.ami_regions
  ami_groups    = var.ami_groups
  source_ami_filter {
    filters = {
      name                = "al2023-ami-2023.*-kernel-*-x86_64"
      root-device-type    = "ebs"
      virtualization-type = "hvm"
    }
    most_recent = true
    owners      = ["amazon"]
  }
  ssh_username = "ec2-user"
}

build {
  name    = "opengris-scaler-build"
  sources = ["source.amazon-ebs.opengris-scaler"]

  provisioner "shell" {
    inline = [
      "sudo dnf update -y",
      "sudo dnf install -y python${var.python_version} python${var.python_version}-pip",
      "sudo python${var.python_version} -m venv /opt/opengris-scaler",
      "sudo /opt/opengris-scaler/bin/python -m pip install --upgrade pip",
      "sudo /opt/opengris-scaler/bin/pip install opengris-scaler==${var.version}",
      "sudo ln -sf /opt/opengris-scaler/bin/scaler_* /usr/local/bin/",
      "sudo ln -sf /opt/opengris-scaler/bin/python /usr/local/bin/opengris-python"
    ]
  }
}

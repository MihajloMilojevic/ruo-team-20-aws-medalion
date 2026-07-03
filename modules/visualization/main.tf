# The visualization module provisions the single EC2 instance that hosts
# PostgreSQL (target of the delivery Lambda) and Apache Superset (dashboard
# UI), both as Docker containers bootstrapped through cloud-init user_data.
#
# Why user_data and not Ansible/anything external: this is a single host with
# a one-time bootstrap, and the IaC requirement is eliminatory — keeping the
# whole setup inside Terraform means one `terraform apply` produces a fully
# working instance with no separate control machine, SSH inventory, or
# out-of-band steps to document and grade.

# Latest Ubuntu 24.04 LTS (amd64) from Canonical's official account.
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# Optional SSH key pair — only created when a public key is supplied in
# terraform.tfvars. Everything on the instance is set up by user_data, so
# SSH is a debugging convenience, not a requirement.
resource "aws_key_pair" "ec2" {
  count = var.ssh_public_key != "" ? 1 : 0

  key_name   = "${var.project_name}-${var.environment}-ec2-key"
  public_key = var.ssh_public_key
}

resource "aws_instance" "visualization" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [var.security_group_id]
  key_name                    = var.ssh_public_key != "" ? aws_key_pair.ec2[0].key_name : null

  # Unlike Lambda, this setting genuinely applies to EC2 — without a public
  # IP the Superset UI and SSH would be unreachable from outside the VPC.
  # Note: the auto-assigned public IP changes when the instance is stopped
  # and started again (an Elastic IP would keep it stable, but public IPv4
  # addresses are billed hourly now, so it's skipped for a class project).
  associate_public_ip_address = true

  # The default 8 GB root volume is too small once the Superset image
  # (~1.5 GB), Postgres, and Docker's own overhead are on it. 20 GB is
  # comfortably inside the 30 GB free-tier EBS allowance.
  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  user_data = templatefile("${path.module}/templates/user_data.sh.tpl", {
    db_username              = var.db_username
    db_password              = var.db_password
    db_name                  = var.db_name
    superset_admin_username  = var.superset_admin_username
    superset_admin_password  = var.superset_admin_password
    superset_secret_key      = var.superset_secret_key
  })

  # user_data only runs on first boot, so a changed bootstrap script must
  # recreate the instance to take effect. Postgres data lives in a Docker
  # volume on the root disk, so recreation wipes it — rerun the delivery
  # Lambda with {"full_refresh": true} afterwards to repopulate.
  user_data_replace_on_change = true

  tags = {
    Name  = "${var.project_name}-${var.environment}-visualization"
    Layer = "Visualization"
  }
}

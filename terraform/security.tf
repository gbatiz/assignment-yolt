resource "aws_security_group" "outbound_all_inbound_ssh_only" {
  name        = "outbound_all_inbound_ssh_only"
  description = "Allow outbound, restric inbound to SSH."
  vpc_id      = aws_vpc.vpc.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

}


resource "aws_key_pair" "ssh_key" {
  key_name   = "ssh_key"
  public_key = var.public_key
}

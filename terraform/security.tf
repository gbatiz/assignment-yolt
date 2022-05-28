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
  public_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC1gKiXyqZl/DW8D58ZZ/xbcOaFThb2lsPAzrzBcZhnD/4xIYGs/jXdOLfvVdqWFo55nOWLAZrp5U+s8Sy1UCPpH++ZFAe8+lvxIz+XFURhJk/Rqf825HsUkujzLQw4j9k+9YkM9ui0Rc0t7q5xNkGjLO3dgPOzoppsSey/baMGFlN2UoQEjC+bZzzzJtwW8iySEOwLj3Ck6ub4A89CMfc8BhAlxmKnSfCI81L63ClKJsFCyJ3ni98i3DPMJyIDAdL6t6PKtXVkrV4zbV8HUsO6M9JyU26/HhTwrZU7WENX3Y8fJ4GWfo3hQg7W/jKbI73hMFDMX3BDVuXKWSqiDe/N gbatiz@gbatiz-thinkpad-arch"
}

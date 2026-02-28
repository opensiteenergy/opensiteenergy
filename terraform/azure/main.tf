terraform {
  required_version = ">=0.12"

  required_providers {
    azapi = {
      source  = "azure/azapi"
      version = "~>1.5"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~>3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "random_pet" "rg_name" {
  prefix = var.resource_group_name_prefix
}

resource "azurerm_resource_group" "rg" {
  location = var.resource_group_location
  name     = random_pet.rg_name.id
}

# Create virtual network
resource "azurerm_virtual_network" "my_terraform_network" {
  name                = "myVnet"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
}

# Create subnet
resource "azurerm_subnet" "my_terraform_subnet" {
  name                 = "mySubnet"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.my_terraform_network.name
  address_prefixes     = ["10.0.1.0/24"]
}

# Create public IPs
resource "azurerm_public_ip" "my_terraform_public_ip" {
  name                = "myPublicIP"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  allocation_method   = "Dynamic"
}

# Create Network Security Group and rule
resource "azurerm_network_security_group" "my_terraform_nsg" {
  name                = "myNetworkSecurityGroup"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  security_rule {
    name                       = "SSH"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
}

# Create network interface
resource "azurerm_network_interface" "my_terraform_nic" {
  name                = "myNIC"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  ip_configuration {
    name                          = "my_nic_configuration"
    subnet_id                     = azurerm_subnet.my_terraform_subnet.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.my_terraform_public_ip.id
  }
}

# Connect the security group to the network interface
resource "azurerm_network_interface_security_group_association" "example" {
  network_interface_id      = azurerm_network_interface.my_terraform_nic.id
  network_security_group_id = azurerm_network_security_group.my_terraform_nsg.id
}

# Generate random text for a unique storage account name
resource "random_id" "random_id" {
  keepers = {
    # Generate a new ID only when a new resource group is defined
    resource_group = azurerm_resource_group.rg.name
  }

  byte_length = 8
}

# Create storage account for boot diagnostics
resource "azurerm_storage_account" "my_storage_account" {
  name                     = "diag${random_id.random_id.hex}"
  location                 = azurerm_resource_group.rg.location
  resource_group_name      = azurerm_resource_group.rg.name
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

# Create virtual machine
resource "azurerm_linux_virtual_machine" "my_terraform_vm" {
  name                  = "myVM"
  location              = azurerm_resource_group.rg.location
  resource_group_name   = azurerm_resource_group.rg.name
  network_interface_ids = [azurerm_network_interface.my_terraform_nic.id]
  size                  = "Standard_DS1_v2"

  os_disk {
    name                 = "myOsDisk"
    caching              = "ReadWrite"
    storage_account_type = "Premium_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }

  computer_name  = "hostname"
  admin_username = var.username

  admin_ssh_key {
    username   = var.username
    public_key = azapi_resource_action.ssh_public_key_gen.output.publicKey
  }

  boot_diagnostics {
    storage_account_uri = azurerm_storage_account.my_storage_account.primary_blob_endpoint
  }
}

resource "random_pet" "ssh_key_name" {
  prefix    = "ssh"
  separator = ""
}

resource "azapi_resource_action" "ssh_public_key_gen" {
  type        = "Microsoft.Compute/sshPublicKeys@2022-11-01"
  resource_id = azapi_resource.ssh_public_key.id
  action      = "generateKeyPair"
  method      = "POST"

  response_export_values = ["publicKey", "privateKey"]
}

resource "azapi_resource" "ssh_public_key" {
  type      = "Microsoft.Compute/sshPublicKeys@2022-11-01"
  name      = random_pet.ssh_key_name.id
  location  = azurerm_resource_group.rg.location
  parent_id = azurerm_resource_group.rg.id
}

output "key_data" {
  value = azapi_resource_action.ssh_public_key_gen.output.publicKey
}

variable "resource_group_location" {
  type        = string
  default     = "northeurope"
  description = "Location of the resource group."
}

variable "resource_group_name_prefix" {
  type        = string
  default     = "rg"
  description = "Prefix of the resource group name that's combined with a random ID so name is unique in your Azure subscription."
}

variable "username" {
  type        = string
  description = "The username for the local account that will be created on the new VM."
  default     = "azureadmin"
}

output "resource_group_name" {
  value = azurerm_resource_group.rg.name
}

output "public_ip_address" {
  value = azurerm_linux_virtual_machine.my_terraform_vm.public_ip_address
}


# resource "aws_instance" "openwindenergy_server" {
#   ami           = "ami-0c4e709339fa8521a"
#   instance_type = "t4g.xlarge"
#   security_groups = ["${aws_security_group.ingress_all_test.id}"]
#   subnet_id = "${aws_subnet.subnet_uno.id}"
#   user_data = <<EOF
# #!/bin/bash
# echo "SERVER_USERNAME=${var.adminname}
# SERVER_PASSWORD=${var.password}" >> /tmp/.env
# sudo apt update -y
# sudo apt install wget -y
# wget https://raw.githubusercontent.com/open-wind/openwindenergy/refs/heads/main/openwindenergy-build-ubuntu.sh
# chmod +x openwindenergy-build-ubuntu.sh
# sudo ./openwindenergy-build-ubuntu.sh
# EOF

#   tags = {
#     Name = "openwindenergy-server"
#   }

#   root_block_device {
#     volume_size = 120
#     volume_type = "gp3"
#     encrypted   = false
#   }
# }

# resource "aws_vpc" "openwindenergy_env" {
#   cidr_block = "10.0.0.0/16"
#   enable_dns_hostnames = true
#   enable_dns_support = true
#   tags = {
#     Name = "openwindenergy_env"
#   }
# }

# resource "aws_eip" "ip_openwindenergy_env" {
#   instance = "${aws_instance.openwindenergy_server.id}"
#   vpc      = true
# }

# resource "aws_internet_gateway" "openwindenergy_env_gw" {
#   vpc_id = "${aws_vpc.openwindenergy_env.id}"
#   tags = {
#     Name = "openwindenergy_env_gw"
#   }
# }

# resource "aws_subnet" "subnet_uno" {
#   cidr_block = "${cidrsubnet(aws_vpc.openwindenergy_env.cidr_block, 3, 1)}"
#   vpc_id = "${aws_vpc.openwindenergy_env.id}"
#   availability_zone = "us-east-1a"
# }

# resource "aws_route_table" "route_table_openwindenergy_env" {
#   vpc_id = "${aws_vpc.openwindenergy_env.id}"
#   route {
#     cidr_block = "0.0.0.0/0"
#     gateway_id = "${aws_internet_gateway.openwindenergy_env_gw.id}"
#   }
#   tags = {
#     Name = "openwindenergy_env_route_table"
#   }
# }
# resource "aws_route_table_association" "subnet_association" {
#   subnet_id      = "${aws_subnet.subnet_uno.id}"
#   route_table_id = "${aws_route_table.route_table_openwindenergy_env.id}"
# }

# resource "aws_security_group" "ingress_all_test" {
#   name = "allow_all_sg"
#   description = "Allow SSH, HTTP and HTTPS"
#   vpc_id = "${aws_vpc.openwindenergy_env.id}"
#   ingress {
#     cidr_blocks = [
#       "0.0.0.0/0"
#     ]
#     from_port = 22
#     to_port = 22
#     protocol = "tcp"
#   }

#   ingress {
#     cidr_blocks = [
#       "0.0.0.0/0"
#     ]
#     from_port = 80
#     to_port = 80
#     protocol = "tcp"
#   }

#   ingress {
#     cidr_blocks = [
#       "0.0.0.0/0"
#     ]
#     from_port = 443
#     to_port = 443
#     protocol = "tcp"
#   }

#   // Terraform removes the default rule
#   egress {
#    from_port = 0
#    to_port = 0
#    protocol = "-1"
#    cidr_blocks = ["0.0.0.0/0"]
#  }
# }

#!/bin/bash

# Bootstrap non-interactive install script to run if not using Terraform

export ADMIN_USERNAME=admin
export ADMIN_PASSWORD=password

echo "ADMIN_USERNAME=${ADMIN_USERNAME}
ADMIN_PASSWORD=${ADMIN_PASSWORD}" >> /tmp/.env
sudo apt update -y
sudo apt install wget -y
wget https://raw.githubusercontent.com/SH801/opensite/refs/heads/main/opensiteenergy-build-ubuntu.sh
chmod +x opensiteenergy-build-ubuntu.sh
sudo ./opensiteenergy-build-ubuntu.sh
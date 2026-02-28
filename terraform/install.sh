#!/bin/bash

# Download and run full Ubuntu installation script from Open Wind Energy GitHub repository
# Ensures any server reboots will run latest install script

sudo apt update -y
sudo apt install wget -y
wget https://raw.githubusercontent.com/open-wind/openwindenergy/refs/heads/main/openwindenergy-build-ubuntu.sh
chmod +x openwindenergy-build-ubuntu.sh
sudo ./openwindenergy-build-ubuntu.sh

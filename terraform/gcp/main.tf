terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.8.0"
    }
  }
}

provider "google" {
  project = var._project
  region  = "us-east1"
  zone    = "us-east1-c"
}

resource "google_compute_address" "openwindenergy_static_ip" {
  name = "ipv4-address"
}

resource "google_compute_network" "openwindenergy_network" {
  name = "openwindenergy-network"
}

resource "google_compute_instance" "vm_instance" {
  name         = "openwindenergy-server"
  machine_type = "c4a-standard-4"
  tags         = ["ssh", "http-server", "https-server"]
  metadata_startup_script = <<EOF
#!/bin/bash
echo "SERVER_USERNAME=${var.adminname}
SERVER_PASSWORD=${var.password}" >> /tmp/.env
sudo apt update -y
sudo apt install wget -y
wget https://raw.githubusercontent.com/open-wind/openwindenergy/refs/heads/main/openwindenergy-build-ubuntu.sh
chmod +x openwindenergy-build-ubuntu.sh
sudo ./openwindenergy-build-ubuntu.sh
EOF


  boot_disk {
    initialize_params {
      image = "ubuntu-2504-plucky-arm64-v20250424"
      size = "120"
    }
  }

  network_interface {
    network = google_compute_network.openwindenergy_network.name
    access_config {
      nat_ip = google_compute_address.openwindenergy_static_ip.address
    }
  }
}

resource "google_compute_firewall" "allow_ssh" {
  name        = "allow-ssh"
  network     = google_compute_network.openwindenergy_network.name
  direction   = "INGRESS"
  priority    = 1000
  target_tags = ["ssh"] # Replace with your instance's target tag
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
  source_ranges = ["0.0.0.0/0"] # Allow from any IP (for testing, restrict in production)
}

resource "google_compute_firewall" "allow_http_https" {
  name          = "allow-http-https"
  network       = google_compute_network.openwindenergy_network.name
  priority      = 1000          
  direction     = "INGRESS"
  source_ranges = ["0.0.0.0/0"]
  allow {
    protocol = "tcp"
    ports    = ["80", "443", "9001"]
  }
  target_tags = ["http-server", "https-server"]
  description = "Allow HTTP and HTTPS traffic"
}


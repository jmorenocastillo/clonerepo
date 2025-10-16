terraform {
  required_providers {
    dummy = {
      source  = "hashicorp/dummy"
      version = "1.0.0"
    }
  }
}

provider "dummy" {}

# Simula crear un servidor
resource "dummy_server" "web" {
  name     = "mi-servidor-web"
  cpu      = 2
  memory   = 4096
  tags = {
    Environment = "production"
    Owner       = "tu-equipo"
  }
}

# Simula crear una base de datos
resource "dummy_database" "app" {
  name     = "mi-db-app"
  size     = 50
  engine   = "mysql"
}
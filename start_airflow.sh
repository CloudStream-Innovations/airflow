#!/bin/bash

# Initialize the Airflow database
echo "Initializing the Airflow database..."
docker-compose up airflow-init

# Build and start Airflow services
echo "Building and starting Airflow services..."
docker-compose up --build -d

# Output success message
echo "Airflow services are up and running. Access the web UI at http://localhost:8080"

FROM apache/airflow:slim-latest-python3.9

# Switch to root to install OS-level dependencies
USER root

# Install PostgreSQL development packages and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the requirements file
COPY requirements.txt /requirements.txt
RUN chown airflow: /requirements.txt

# Switch back to the airflow user and install Python dependencies
USER airflow
RUN pip install -r /requirements.txt

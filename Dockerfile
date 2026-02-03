# Use official Airflow image as base
FROM apache/airflow:3.0.3

# Switch to airflow user to install packages
USER airflow

# Explictly specify the current working directory
WORKDIR /opt/airflow

# Copy requirements.txt into image
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
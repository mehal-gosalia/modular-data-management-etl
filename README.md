# Modular Data Management & ETL Pipelines

## Purpose
Modern software systems operate across multiple environments such as development, testing, sandbox, UAT, staging, and production. To build and debug features effectively, engineers need access to production-like data. However, production databases often contain sensitive or confidential information and cannot be safely shared.

This project implements a configurable, batch-oriented ETL data pipeline that copies production data while sanitizing, obfuscating, or minimizing sensitive fields before distributing it to non-production environments. The goal is to preserve data realism while ensuring privacy, compliance, and efficient resource usage.

---

## ETL Pipeline Diagram (DAG)
The ETL workflow is modeled as a Directed Acyclic Graph (DAG) and orchestrated using Apache Airflow. Each node represents a discrete transformation stage executed in a deterministic order.

---

## System Architecture
The system is designed as a modular, containerized ETL platform:
- Configuration-driven pipeline behavior
- Environment-isolated databases
- Reusable transformation modules
- Orchestration and scheduling via Apache Airflow

> System architecture diagram: *In progress*

---

## Features
- Dynamic rule engine for flexible pipeline behavior
- YAML-driven ETL pipeline definition
- Secure data copying across MongoDB databases
- Sanitization and obfuscation of sensitive fields
- Data minimization to reduce storage and compute costs
- Airflow-based orchestration using industry standards
- Batch processing with memory-aware sizing
- Fully containerized execution using Docker

---

## Tech Stack
- **Language:** Python  
- **Linting / IDE:** Pylance  
- **Databases:** MongoDB (PyMongo)  
- **Synthetic Data:** Faker  
- **Orchestration:** Apache Airflow  
- **Containerization:** Docker, Docker Compose  
- **Configuration:** YAML  
- **Testing:** Unittest  

---

## YAML Configuration Rules

### Description
YAML configuration files act as templates for the dynamic rule engine. They define the behavior of each pipeline stage, allowing fine-grained control over transformations at the database, collection, and field level.

The ETL engine interprets these rules and executes predefined transformation logic without requiring code changes.

---

### Configuration Format
- `pipeline` – Declares a pipeline configuration
- `step` – Description of the pipeline stage
- `type` – Transformation module  
  - `copy`
  - `sanitize_obfuscate`
  - `minimize`
  - `refresh` *(planned)*
- `order` – Execution order of the stage
- `source_db` – Source database
- `dest_db` – Destination database
- `collections` – Target collections
- `fields` – Target fields per collection
- `strategy` – Transformation strategy  
  - Sanitization / Obfuscation:
    - `redact`
    - `substitute`
    - `perturb`
    - `hash_salt`
  - Minimization:
    - `sample`
- `params` – Strategy-specific parameters

---

### Example YAML Configuration
```yaml
pipelines:
  - step: sanitize_clean
    type: sanitize_obfuscate
    order: 2
    source_db: financial_data_clean
    dest_db: financial_data_clean
    collections:
      customers:
        username:
          strategy: substitute
          params:
            field_type: username

For a complete configuration example, refer to the YAML files located in the `configs/` directory.

---

## Current Functionality
- Configuration-driven ETL execution
- Field-level sanitization and obfuscation
- Selective data minimization
- Environment-to-environment database replication
- Memory-safe batch processing

---

## How to Run the Pipeline
Documentation for running the pipeline locally using Docker and Apache Airflow is currently under development.

---

## Future Development
- Data refresh pipeline module
- Expanded sanitization, obfuscation, and minimization strategies
- Support for nested and hierarchical data fields
- Data validation using Pydantic
- Privacy-compliant pipelines aligned with governance standards
- Container orchestration using Kubernetes or Docker Swarm
- Cloud-based deployment for Apache Airflow and MongoDB
- PostgreSQL-based pipeline support
- Data warehouse integration for analytics and dashboards


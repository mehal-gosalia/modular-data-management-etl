Modular Data Management & ETL Pipelines
Overview & Motivation

Modern software systems typically operate across multiple environments—development, testing, sandbox, UAT, staging, and production. For effective development and debugging, engineers need access to realistic, production-like data. However, production databases often contain sensitive or confidential information, making direct usage unsafe or non-compliant.

This project implements a modular, configurable ETL data pipeline that automates the process of copying production data while sanitizing, obfuscating, or minimizing sensitive fields before distributing it to non-production environments. The pipeline is designed to balance data realism, privacy, performance, and cost efficiency, ensuring downstream environments receive high-quality data without exposing confidential information.

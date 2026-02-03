# Modular Data Management & ETL Pipelines

## Purpose
Modern software systems operate across multiple environments such as development, testing, sandbox, UAT, staging, and production. To build and debug features effectively, engineers need access to production-like data. However, production databases often contain sensitive or confidential information and cannot be safely shared.

This project implements a configurable, batch-oriented ETL data pipeline that copies production data while sanitizing, obfuscating, or minimizing sensitive fields before distributing it to non-production environments. The goal is to preserve data realism while ensuring privacy, compliance, and efficient resource usage.

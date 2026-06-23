# Technical Architecture Decisions (ADR)

This document records the main architectural and design decisions made during the development of the ETL pipeline for DataMart S.A.S.

## 1. Cluster Architecture: Why CeleryExecutor over LocalExecutor?

For this project's orchestration, we chose to deploy an Apache Airflow cluster using **CeleryExecutor** backed by **RabbitMQ** as the message broker and multiple *worker* nodes, instead of a simpler setup like *LocalExecutor*. This decision is based on the following professional criteria:

### 1.1 AWS Cloud Readiness & EC2 Distribution
The main vision for this architecture is to be fully ready for deployment in a cloud environment like **AWS**. By decoupling the services and their `.env` files, we can easily split this `docker-compose` setup across multiple **EC2 instances**. To migrate a service to its own dedicated machine, we only need its compose block and its specific `.env` file. Then, we simply replace the service name with the new node's private IP. This modularity is the reason behind our separated environment files (e.g., `.env.master`, `.env.worker`, `.env.broker`).

### 1.2 Security & Network Isolation
We designed the cluster prioritizing security and isolation:
* **Nginx Proxy Manager (NPM):** We use a reverse proxy to manage incoming traffic safely. This prevents exposing our internal private network and core services directly to the public internet.
* **Private Metadata Database:** The Airflow metadata is stored in a dedicated, isolated PostgreSQL database that is only accessible internally by the cluster.

### 1.3 True Horizontal Scalability
While `LocalExecutor` runs tasks as subprocesses on the same machine as the Scheduler (limited by a single node's resources), `CeleryExecutor` allows **horizontal scaling**. As DataMart's transaction volume grows (given its 40% annual growth), we can easily add more worker nodes across different servers to process data in parallel, without modifying the core orchestrator.

### 1.4 Fault Tolerance
Heavy data extraction and transformation tasks can consume large amounts of RAM and cause unexpected crashes.
* **With LocalExecutor:** An Out-Of-Memory (OOM) error on a heavy task can crash the main container, taking down the Scheduler and stopping the entire platform.
* **With CeleryExecutor:** There is strict resource isolation. If a *worker* crashes, the Scheduler and Webserver continue working normally. The broker (RabbitMQ) will simply reassign the failed task to a healthy worker or trigger the DAG's retry policy.

---
*(This directory will also include documents explaining business rules decisions, analytical data modeling, and DAG idempotency).*

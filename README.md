# README

This repository contains the DAGs (Directed Acyclic Graphs) for our ETL Data Pipeline Proof of Concept.

## Important Limitation :collision:

The current implementation of the DAGs and dependency management is not optimal for scalability. An ideal approach is discussed in this [video](https://youtu.be/uA-8Lj1RNgw?si=4kIWdpaJ_2gI-qnR).

Implementing this ideal solution would require a more sophisticated Airflow deployment, beyond the scope of this PoC. This would involve using ECS (Elastic Container Service) and EKS (Elastic Kubernetes Service) to run Airflow.

A note regarding this limitation is listed in the [issues](https://github.com/orgs/CloudStream-Innovations/projects/1?pane=issue&itemId=64959428) section, but it will not be addressed for the purposes of this PoC.

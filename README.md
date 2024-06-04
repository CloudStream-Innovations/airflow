# README

This repository contains the DAGs (Directed Acyclic Graphs) for our ETL Data Pipeline Proof of Concept.

## NOTE ON PROGRESS

These are not yet production-ready. I still need to sit and work through them, troubleshoot, and ensure that all dependencies are correct. These scripts are meant to indicate how the next stage in the process would work when taking the notebooks to production. They will also be tied into a CI/CD process to publish results to the output repository and then to the website. I need a day or two to get these working properly, as Airflow can be tedious to troubleshoot.

The required development infrastructure is in place with Docker Compose, and the IaC for the solution is also available in the infrastructure repository. However, this also needs troubleshooting due to a hard cap error at the account level.

## Important Limitation :collision:

The current implementation of the DAGs and dependency management is not optimal for scalability. An ideal approach is discussed in this [video](https://youtu.be/uA-8Lj1RNgw?si=4kIWdpaJ_2gI-qnR).

Implementing this ideal solution would require a more sophisticated Airflow deployment, beyond the scope of this PoC. This would involve using ECS (Elastic Container Service) and EKS (Elastic Kubernetes Service) to run Airflow.

A note regarding this limitation is listed in the [issues](https://github.com/orgs/CloudStream-Innovations/projects/1?pane=issue&itemId=64959428) section, but it will not be addressed for the purposes of this PoC.

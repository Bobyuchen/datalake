"""
This file has been generated from dag_runner.j2
"""
from airflow import DAG
from openmetadata_managed_apis.workflows import workflow_factory

workflow = workflow_factory.WorkflowFactory.create("/opt/airflow/dag_generated_configs/581ab1e3-af05-42a9-82a4-f2bfbe5769f0.json")
workflow.generate_dag(globals())
dag = workflow.get_dag()
"""
This file has been generated from dag_runner.j2
"""
from airflow import DAG
from openmetadata_managed_apis.workflows import workflow_factory

workflow = workflow_factory.WorkflowFactory.create("/opt/airflow/dag_generated_configs/b49fded4-8fe3-4281-ae1a-7e8c4c175a1e.json")
workflow.generate_dag(globals())
dag = workflow.get_dag()
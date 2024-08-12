"""
This file has been generated from dag_runner.j2
"""
from airflow import DAG
from openmetadata_managed_apis.workflows import workflow_factory

workflow = workflow_factory.WorkflowFactory.create("/opt/airflow/dag_generated_configs/06ee944e-a806-4596-92bd-8793354abf24.json")
workflow.generate_dag(globals())
dag = workflow.get_dag()
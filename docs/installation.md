## Run the Local datalake Server

Before diving into the specifics of data transformation and analysis with Dbt, it's essential to have the Trino server up and running. Trino serves as a distributed SQL query engine that allows you to query your data across different sources seamlessly. Here's how to start the Trino server locally using Docker:

```
cd docker
docker-compose up --build -d
```

## Run the Dbt and dagster Commands

Activate the virtual environment and set up dbt and Dagster

```
cd dbt_omop
python -m venv omop_venv  

source omop_venv/bin/activate         # activate the environment for Mac and Linux OR
omop_venv\Scripts\activate            # activate the environment for Windows

python -m pip install dbt-trino
pip install dagster-dbt dagster-webserver pandas
```
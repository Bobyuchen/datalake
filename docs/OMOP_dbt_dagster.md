## Activate the virtual environment

```
cd dbt_omop

source omop_venv/bin/activate         # activate the environment for Mac and Linux OR
omop_venv\Scripts\activate            # activate the environment for Windows
```

## Operate dbt

```
dbt deps
dbt debug
python Transform_ice.py               # Convert JSON file to rowdata in Iceberg format
dbt run --models omop_sources
dbt run --models omop_stage
dbt run
```

## Operate Dagster

```
cd omop_dagster
dagster dev
# In your browser, navigate to http://127.0.0.1:3000. 
```
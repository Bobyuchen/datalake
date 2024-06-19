
# Iceberg + Dbt + Trino + Hive: Modern, Open-Source Data Stack

![](https://cdn-images-1.medium.com/max/3086/1*Hg-ZHsBd_yJ54legC570oA.png)


The repository showcases a demo of integrating Iceberg, Dbt, Trino, and Hive, forming a modern and open-source data stack suitable for various analytical needs. This guide provides a structured approach to setting up and utilizing this stack effectively, ensuring a seamless workflow from data ingestion to analysis.

## Run the Local Trino Server

Before diving into the specifics of data transformation and analysis with Dbt, it's essential to have the Trino server up and running. Trino serves as a distributed SQL query engine that allows you to query your data across different sources seamlessly. Here's how to start the Trino server locally using Docker:


```
cd docker
docker-compose up --build -d
```

This command navigates to the Docker directory within your project and initiates the Docker Compose process, which builds and starts the containers defined in your `docker-compose.yml` file in detached mode.

## Containers

主要的containers
1. **trino**: 可以開localhost UI。
   ```
   docker exec -it trino trino
   ```
   就可以執行sql語法 ex:
   ```
   show catalogs;
   show schemas from datalake;
   show tables from datalake.analytics_stage;
   select * from datalake.analytics_stage.stg_streams_hourly;
   ```
2. **mongo**: UI要另外下載MongoDB Compass。
3. **oltp**: postgresql。POSTGRES_DB=postgres。POSTGRES_USER=postgres。POSTGRES_PASSWORD=postgres
   ```
   docker exec -it oltp psql -U postgres -d postgres
   ```

4. **metastore_db**: postgresql。POSTGRES_DB=metastore。POSTGRES_USER=hive。POSTGRES_PASSWORD=hive
   ```
   docker exec -it metastore_db psql -U hive -d metastore
   ```

5. **minio**:可以開localhost UI，帳號minio，密碼minio123。
6. **broker & schema-registry**: 兩個是Kafka server啟動compose有時候可能會關掉，要再開一次。

## Trino catalog

會有5個catalog，可以由tirno連線。會存在trino container Files中的etc/trino/catalog
1. **datalake**:指向minio經由iceberg表格式管理。
2. **hive**:指向minio經由hive表格式管理。
3. **metastore_db**:指向metastore_db。為postgresql資料庫。儲存metadata。
4. **oltp**:指向oltp。為postgresql的資料庫，儲存資料。
5. **website**:mongodb。為mongodb資料庫，儲存資料。


## Integration with Kafka for Data Streaming

To simulate real-time data streaming in a music event context, follow the instructions from the GitHub repository [Stefen-Taime/eventmusic](https://github.com/Stefen-Taime/eventmusic.git). This repository contains scripts and configurations necessary for producing messages to Kafka, which acts as the backbone for real-time data handling in this stack.(可以略過這段使用以下方法創建資料)

### Preparing Kafka Connectors

After setting up the Docker containers and running the local Trino server, proceed with the Kafka connectors setup:
   
1. **Set Permissions for `install_connectors.sh`**: This script installs the necessary Kafka connectors for integrating with PostgreSQL and MongoDB. Adjust the file permissions to make it executable.
   ```
   cd docker
   chmod +x install_connectors.sh
   ```
   
2. **Execute `install_connectors.sh`**: Run the script to install the Kafka connectors.
   ```
   ./install_connectors.sh
   ```

### Configuring Connectors and Producing Data

With the connectors installed:

1. **Set Permissions for `postConnect.sh`**: This script configures the connectors. Modify the permissions to ensure executability.
   ```
   chmod +x postConnect.sh
   ```
   
2. **Execute `postConnect.sh`**: Run the script to configure the connectors and initiate data streaming.
   ```
   ./postConnect.sh
   ```

## Run the Dbt Commands

起虛擬環境抓dbt-trino件
```
cd dbt
python -m venv dbt-env  

source dbt-env/bin/activate         # activate the environment for Mac and Linux OR
dbt-env\Scripts\activate            # activate the environment for Windows

python -m pip install dbt-trino
```

check point
```
dbt --version
---------------
Core:
  - installed: 1.8.2
  - latest:    1.8.2 - Up to date!

Plugins:
  - trino: 1.8.0 - Up to date!
```

With the Trino server running, the next step is to execute the necessary Dbt commands to manage your data transformations:

```
dbt deps
dbt debug
dbt run --models source
dbt run --models stage
dbt run --models mart
dbt run
```

`dbt deps` fetches the project's dependencies, ensuring that all required packages and modules are available.
`dbt debug` will All checks passed.
`dbt run` then executes the transformations defined in your dbt project, building your data models according to the specifications in your dbt files.PASS=15 WARN=0 ERROR=0 SKIP=0 TOTAL=15.

## Get Superset

To get started with Apache Superset, follow these steps to pull and run the Superset Docker image. Ensure you have Docker installed and running on your machine.

1. **Set Superset Version**:
   Set the `SUPERSET_VERSION` environment variable with the latest Superset version. Check the [Apache Superset releases](https://github.com/apache/superset/releases) for the latest version.
   ```
   export SUPERSET_VERSION=<latest_version>
   ```

2. **Pull Superset Image**:
   Pull the Superset image from Docker Hub.
   ```
   docker pull apache/superset:$SUPERSET_VERSION
   ```

3. **Start Superset**:
   Note that Superset requires a user-specified value of `SECRET_KEY` or `SUPERSET_SECRET_KEY` as an environment variable to start.
   ```
   docker run -d -p 3000:8088 \
              -e "SUPERSET_SECRET_KEY=$(openssl rand -base64 42)" \
              -e "TALISMAN_ENABLED=False" \
              --name superset apache/superset:$SUPERSET_VERSION
   ```

4. **Create an Account**:
   Create an admin account in Superset.
   ```
   docker exec -it superset superset fab create-admin \
               --username admin \
               --firstname Admin \
               --lastname Admin \
               --email admin@localhost \
               --password admin
   ```

5. **Configure Superset**:
   Configure the database and load example data.
   ```
   docker exec -it superset superset db upgrade && \
          docker exec -it superset superset load_examples && \
          docker exec -it superset superset init
   ```
   
6. **Start Using Superset**:
   After configuration, access Superset at `http://localhost:8080` with the default credentials:
   - Username: `admin`
   - Password: `admin`

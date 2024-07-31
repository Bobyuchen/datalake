
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

## Main Containers

主要的containers
1. **trino**: 可以開localhost UI，沒有權限問題。
   ```
   docker exec -it trino trino
   ```
   就可以執行sql語法 ex:
   ```
   SHOW catalogs;
   SHOW schemas FROM datalake;
   SHOW tables FROM datalake.analytics_stage;
   SELECT * FROM datalake.analytics_stage.stg_streams_hourly;
   SELECT COUNT(*) AS row_count FROM oltp.public.auth_events; #確認postgresql source資料來源有無更新。
   SELECT COUNT(*) AS row_count FROM datalake.analytics_source.src_page_view_events; #可以確認數據在datalake有無更新
   SELECT * FROM datalake.analytics_source."src_page_view_events$snapshots";  #撈iceberg版本紀錄
   SELECT * FROM [db_name.]table_name FOR TIMESTAMP AS OF timestamp;          #版本歷程查詢
   SELECT * FROM [db_name.]table_name FOR VERSION AS OF version;              #版本歷程查詢
   ```
2. **mongo**: UI要另外下載MongoDB Compass。
3. **postgresql**: postgresql。POSTGRES_DB=postgres。POSTGRES_USER=postgres。POSTGRES_PASSWORD=password
   ```
   docker exec -it postgresql psql -U postgres -d postgres
   SELECT COUNT(*) FROM auth_events;                    #進postgresql查看source。
   ```

4. **metastore**: postgresql。POSTGRES_DB=metadata。POSTGRES_USER=postgres。POSTGRES_PASSWORD=password
   ```
   docker exec -it metastore psql -U postgres -d metadata
   ```

5. **minio**:可以開localhost UI，帳號minio，密碼minio123。
6. **iceberg-rest**: iceberg REST Server。
7. **openmetadata_elasticsearch**: 為OpenMetadata儲存metadata及搜尋內部metadata引擎。
8. **execute_migrate_all**: 用於初始化及更新OpenMetadata數據庫架構。
9. **openmetadata_server**: OpenMetadata server，負責OpenMetadata核心服務，提供API接口、管理元數據存儲、執行身份驗證和授權等功能。
8. **openmetadata_ingestion**: OpenMetadata的數據攝取服務，負責從各種數據源中提取metadata。

## Trino catalog

Trino有5個catalog，可以由tirno連線。會存在trino container Files中的etc/trino/catalog
1. **iceberg**:指向minio經由iceberg表格式管理。
2. **metastore**:指向metastore。為postgresql資料庫。儲存metadata。
3. **postgresql**:指向postgresql。為postgresql的資料庫，儲存資料。
4. **mongo**:mongodb。為mongodb資料庫，儲存資料。

## Initialize `test` schema

There is a simple SQL script to initialize a test schema
in the `iceberg` catalog by copying TPCH "tiny" schema from the Trino:

```bash
docker exec -it trino trino -f /home/test/test-schema.sql
```

This is not required. It is possible to create other schemata in the `iceberg`
catalog and create and populate tables there in any way.


## Integration with Kafka for Data Streaming

To simulate real-time data streaming in a music event context.

### Preparing Kafka Connectors

After setting up the Docker containers and running the local Trino server, proceed with the Kafka connectors setup:(要等container都啟動完，設定Kafka連線權限，並確認broker & schema-registry兩個container都有開啟)
   
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

## EventMusic Producer

EventMusic Producer is a Dockerized application designed to read data and output them to a Kafka topic, using Avro schemas for data serialization. It integrates seamlessly with Kafka and the Schema Registry to manage the flow of event data linked to music event information.(啟動資料流，詳細可看eventmusic-main\READMEeventmusic.md，有自動與手動)

### Pull the Docker Image

```
docker pull stefen2020/eventmusic:latest
```

### Run the Container

Make sure Kafka and Schema Registry are running and accessible. 這個Container啟動，會自動每30秒發送一次5 batch size.

```
docker run --network="host" --name eventmusic-container stefen2020/eventmusic:latest
```

## Getting Started manually

Make sure Kafka and Schema Registry are running and accessible. 手動直接執行.py就會直接發送一次。

```
python listen_events.py

python page_view_events.py

python auth_events.py

python main.py
```


## Run the Dbt Commands

起虛擬環境抓dbt-trino件
```
cd dbt
python -m venv dbt-venv  

source dbt-venv/bin/activate         # activate the environment for Mac and Linux OR
dbt-venv\Scripts\activate            # activate the environment for Windows

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

dbt對iceberg materialized格式

1. **table**: dbt table的邏輯是沒有table就創建一個新的，有舊的table存在會刪掉舊的，建一個新的。所以對iceberg而言，不會有版本紀錄，舊的跟新的本質是兩個不同的。

2. **incremental**:dbt incremental的邏輯是只處理有變動的部分，不會更新整個表，當有資料新增，就對舊的表去新增資料，所以會有iceberg的版本紀錄。


## Using OpenMetadata
https://docs.open-metadata.org/v1.4.x/quick-start/local-docker-deployment
1. **Log in to OpenMetadata**:
OpenMetadata provides a default admin account to login.You can access OpenMetadata at http://localhost:8585. Use the following credentials to log in to OpenMetadata.
   Username: admin@openmetadata.org
   Password: admin
Once you log in, you can goto Settings -> Users to add another user and make them admin as well.

2. **Log in to Airflow**:
OpenMetadata ships with an Airflow container to run the ingestion workflows that have been deployed via the UI.In the Airflow, you will also see some sample DAGs that will ingest sample data and serve as an example.You can access Airflow at http://localhost:8080. Use the following credentials to log in to Airflow.
   Username: admin
   Password: admin


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

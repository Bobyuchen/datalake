import json
import pandas as pd
import trino
import glob
import os

# 連接到 Trino
conn = trino.dbapi.connect(
    host='localhost',
    port=8080,
    user='the-user',
    catalog='iceberg',
    schema='ifhir'
)
cur = conn.cursor()
cur.execute('DROP SCHEMA IF EXISTS iceberg.ifhir CASCADE')
cur.execute('CREATE SCHEMA IF NOT EXISTS iceberg.ifhir')

# 讀取指定路徑下的所有 JSON 文件
file_paths = glob.glob('C:/Users/ASUS/Desktop/work/datalake/dbt-omop/FHIR_rowdata/twpas_output_transaction_20240813_*.json')

# 初始化存儲所有數據的列表
all_entries = []

# 讀取每個 JSON 檔案並提取所有資料
for file_path in file_paths:
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    for entry in data['entry']:
        flattened_data = {
            'request_method': entry['request']['method'],
            'request_url': entry['request']['url'],
            'resource_id': entry['resource'].get('id', ''),
            'resource_type': entry['resource'].get('resourceType', ''),
            'resource_status': entry['resource'].get('status', ''),
            'full_url': entry.get('fullUrl', '')
        }

        # 深入提取每個字段
        for key, value in entry['resource'].items():
            if isinstance(value, dict):  # 對象類型的字段
                flattened_data[key] = json.dumps(value)  # 將 dict 轉換為 JSON 字符串
            elif isinstance(value, list):  # 列表類型的字段
                flattened_data[key] = json.dumps(value)  # 將 list 轉換為 JSON 字符串
            else:
                flattened_data[key] = value

        all_entries.append(flattened_data)

# 將所有 entries 轉換為 DataFrame
df_all = pd.DataFrame(all_entries)

# 將 DataFrame 中的 NaN 值替換為 None，以便在 SQL 中插入 NULL
df_all = df_all.where(pd.notnull(df_all), None)

# 根據 request_url 的前綴部分分組
df_all['prefix'] = df_all['request_url'].apply(lambda x: x.split('/')[0].lower())
grouped_data = df_all.groupby('prefix')

# 遍歷每個分組創建表格並插入數據
for prefix, group_df in grouped_data:
    # 取出表名
    table_name = prefix

    # 構建 CREATE TABLE 語句
    columns = []
    for column_name, dtype in group_df.dtypes.items():
        # 映射 pandas 資料類型到 SQL 類型
        if dtype == 'int64':
            sql_type = 'bigint'
        elif dtype == 'float64':
            sql_type = 'double'
        elif dtype == 'bool':
            sql_type = 'boolean'
        elif dtype == 'datetime64[ns]':
            sql_type = 'timestamp'
        else:
            sql_type = 'varchar'  # 默認為 varchar

        columns.append(f"{column_name} {sql_type}")

    columns_sql = ",\n  ".join(columns)

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS iceberg.ifhir.{table_name} (
      {columns_sql}
    )
    WITH (format = 'ORC')
    """

    # 執行創建表格的語句
    cur.execute(create_table_sql)

    # 構建 INSERT 語句
    for _, row in group_df.iterrows():
        column_names = ", ".join(row.index)
        values = ", ".join(
            [f"NULL" if pd.isna(v) else f"'{str(v).replace('\'', '\'\'')}'" if isinstance(v, str) else str(v) for v in row.values]
        )
        insert_sql = f"""
        INSERT INTO iceberg.ifhir.{table_name} ({column_names})
        VALUES ({values})
        """
        cur.execute(insert_sql)

print("Data inserted into Iceberg tables successfully!")

import os
import json
import pandas as pd
import trino

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

#使用絕對路徑
with open('C:/Users/ASUS/Desktop/work/datalake/dbt_omop/FHIR_rowdata/twpas_output_transaction_20240813.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# 使用相對路徑
# file_path = os.path.join('FHIR_rowdata', 'twpas_output_transaction_20240813.json')

# with open(file_path, 'r', encoding='utf-8') as file:
#     data = json.load(file)

# 初始化存儲分組數據的字典
grouped_data = {}

# 遍歷每個 entry 並按前綴分組
for entry in data['entry']:
    url = entry['request']['url']
    prefix = url.split('/')[0].lower()  # 獲取URL的前綴部分

    if prefix not in grouped_data:
        grouped_data[prefix] = []

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

    # 添加數據到分組字典
    grouped_data[prefix].append(flattened_data)

# 動態創建 Iceberg 表格並插入數據
for prefix, entries in grouped_data.items():
    df = pd.DataFrame(entries)

    # 將 DataFrame 中的 NaN 值替換為 None，以便在 SQL 中插入 NULL
    df = df.where(pd.notnull(df), None)

    columns = []

    # 根據 DataFrame 的結構來創建 CREATE TABLE 語句
    for column_name, dtype in df.dtypes.items():
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

    # 建表語句
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS iceberg.ifhir.{prefix} (
      {columns_sql}
    )
    WITH (format = 'ORC')
    """
    
    # 執行創建表格的語句
    cur.execute(create_table_sql)
    
    # 將數據插入到表中
    for _, row in df.iterrows():
        # 動態生成 INSERT 語句
        column_names = ", ".join(row.index)
        values = ", ".join(
            [f"NULL" if pd.isna(v) else f"'{str(v).replace('\'', '\'\'')}'" if isinstance(v, str) else str(v) for v in row.values]
        )
        insert_sql = f"""
        INSERT INTO iceberg.ifhir.{prefix} ({column_names})
        VALUES ({values})
        """
        cur.execute(insert_sql)

print("Data inserted into Iceberg tables successfully!")

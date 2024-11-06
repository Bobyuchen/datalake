import json
import pandas as pd
from sqlalchemy import create_engine

# 建立 PostgreSQL 連線
engine = create_engine('postgresql+psycopg2://postgres:password@127.0.0.1:5432/postgres')
schema_name = 'dbt'

# 讀取 JSON 文件
with open('C:/Users/ASUS/Desktop/work/datalake/dbt-omop/twpas_output_transaction_20240813.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

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

# 將分組數據寫入 PostgreSQL 資料庫
for prefix, entries in grouped_data.items():
    df = pd.DataFrame(entries)
    # 將 DataFrame 存儲到指定 schema 的 PostgreSQL 資料庫中
    table_name = f"fhir_data_{prefix}".lower()  # 將表格名稱轉換為小寫
    df.to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False)

print("Data inserted into PostgreSQL successfully!")

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import subprocess
from pydantic import BaseModel
from google.cloud import storage
import psycopg2
import json
from psycopg2 import sql
from datetime import date
import os
import atexit
from datetime import datetime

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允許的來源 URL
    allow_methods=["*"],  # 允許的 HTTP 方法
    allow_headers=["*"],  # 允許的請求標頭
    allow_credentials=True,  # 是否允許攜帶憑證
    expose_headers=["Content-Disposition"],  # 可顯示的回應標頭
)


connection = psycopg2.connect(
    host='127.0.0.1',
    user='presto',
    password='Wyuwdymijh%0',
    database='postgres'
)

# 指定存儲桶名稱、要下載的對象路徑，以及要將資料保存到的本地文件路徑
bucket_name = f"test_buckets001"
directory_path = "DBT"
download_folder = "download"

# DBT 模型
class DBTRunRequest(BaseModel):
    model : str
    type : str
    
class BaseRequest(BaseModel):
    BaseRequest : str

class BaseResource(BaseModel):
    BaseResource : str

class DBTRunResponse(BaseModel):
    stdout: str
    stderr: str
    returncode: int





# 請求頁面
@app.post('/dbt/run', response_model=DBTRunResponse)
async def run_dbt(request: DBTRunRequest):
    # 获取请求中的参数
    model = request.model
    type = request.type
    
    # 构建dbt命令
    command = ['dbt', 'run']
    
    command.extend(['--profiles-dir','.'])
    
    if type == 'full':
        command.append('--full-refresh')
    elif type == 'exclude':
        command.extend(['--exclude', model])
    else:
        command.extend(['--models', model])
    
    
    # 执行dbt命令
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    # 返回结果
    return DBTRunResponse(stdout=stdout.decode('utf-8'), stderr=stderr.decode('utf-8'), returncode=process.returncode)

@app.get('/dbt/seed')
async def seed_dbt():
    
    # 构建dbt命令
    command = ['dbt', 'seed']

    # 执行dbt命令
    result = subprocess.run(command, capture_output=True, text=True)

    # 返回结果
    return DBTRunResponse(stdout=result.stdout, stderr=result.stderr, returncode=result.returncode)

@app.get('/dbt/snapshots')
async def snapshots_dbt():
    
    # 构建dbt命令
    command = ['dbt', 'snapshot']

    # 执行dbt命令
    result = subprocess.run(command, capture_output=True, text=True)

    # 返回结果
    return DBTRunResponse(stdout=result.stdout, stderr=result.stderr, returncode=result.returncode)

@app.post('/test')
async def test(): 
    print(f'測試通道完成')
    get_time()
    
@app.post('/bucket_to_db')
async def bucket_to_db(): #將下載資訊寫入db
    clear_table('p_origin')
    #bucket連線
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=directory_path)
    
    for blob in blobs:
        if blob.name.endswith(".json"):
            # 下載 JSON 檔案內容至字串
            json_string = blob.download_as_string()
            json_str_decode = json_string.decode('utf-8')
            json_data = json.loads(json_str_decode)
            json_str = json.dumps(json_data)
            #寫入DB
            update_json(json_str)
    get_time()
    
@app.post('/dbt_exec')
async def dbt_exec(): #執行dbt 
    responses = []
    print('start dbt run row_data.')
    data1 = DBTRunRequest(model='row_data', type='select')
    response_1 = await run_dbt(data1)
    responses.append(response_1)
    print('row_data  done.')
    
    print('start dbt run all(exclude row_data).')
    data2 = DBTRunRequest(model='row_data', type='exclude')
    response_2 = await run_dbt(data2)
    responses.append(response_2)
    print('all done.')
    get_time()
    print('------------------------------------------------------------------------')
    return {"BaseResource": "OK", "response": responses}


def download_blobs(bucket_name, directory_path,download_folder):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    
    # 取得 bucket 中的所有對象
    blobs = bucket.list_blobs(prefix=directory_path)
    
    for blob in blobs:
        if blob.name.endswith(".json"):
            # 下載 JSON 檔案
            local_file_path = os.path.join(download_folder, os.path.basename(blob.name))
            blob.download_to_filename(local_file_path)
            print(f"Blob {local_file_path} downloaded from {bucket_name}/{directory_path}.")
    
    
# 上傳json到psql
def update_json(json_str):
    cursor = connection.cursor()
    
    # 构建插入数据的SQL查询
    insert_query = sql.SQL("INSERT INTO dbt.P_origin (data, insert_date) VALUES (%s, %s);")
    current_date = date.today().isoformat()
    cursor.execute(insert_query, (json_str,current_date))

    connection.commit()  
     
    cursor.close()
    
def clear_table(table_name):
    cursor = connection.cursor()
    
    # 构建删除数据的SQL查询
    delete_query = sql.SQL("DELETE FROM dbt.{}").format(sql.Identifier(table_name))
    
    # 执行删除查询
    cursor.execute(delete_query)
    connection.commit()
    cursor.close()
    
def get_time():
    # 获取当前时间
    current_time = datetime.now()

    # 格式化为字符串形式，包括到秒的精确时间
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

    print("Current time:", formatted_time)    
    return formatted_time

# 注册在应用程序关闭时执行的函数
def close_db_connection():
    connection.close()
    print('--------------------db link stop.---------------------')

    
atexit.register(close_db_connection)
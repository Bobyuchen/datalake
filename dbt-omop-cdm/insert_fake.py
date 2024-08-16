import psycopg2
import json
from psycopg2 import sql
from datetime import date

#連接postgresql資料庫
connection = psycopg2.connect(
    host='127.0.0.1',
    user='presto',
    password='Wyuwdymijh%0',
    database='postgres'
)

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
   
    
clear_table('p_origin')

row = 1
loop_list = ['response.json']#,'response2.json','response3.json','response4.json','response5.json','response6.json']
for file in loop_list:
    with open( f'./download/{file}' , 'r',encoding='utf-8') as file:
        json_data = json.load(file)
        # entry = json_data['RETURN_DATA']['FHIR_OBJ']['entry']
    json_str = json.dumps(json_data)


    update_json(json_str)

    
connection.close()
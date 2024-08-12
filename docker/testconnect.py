import trino
conn = trino.dbapi.connect(
    host='localhost',
    port=8080,
    user='the-user',
    # catalog='system',
    # schema='information_schema',
)
cur = conn.cursor()
cur.execute('show tables from iceberg.test')
rows = cur.fetchall()
print(rows)
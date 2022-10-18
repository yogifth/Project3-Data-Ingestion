import json
import psycopg2 as pg

schema_json = '/mnt/e/Yogi/Data Engineer/Document/project_3_de/sql/schemas/user_address.json'

with open(schema_json, 'r') as schema:
    content = json.loads(schema.read())

list_schema = []
for c in content:
     col_name = c['column_name']
     col_type = c['column_type']
     constraint = c['is_null_able']
     ddl_list = [col_name, col_type, constraint]
     list_schema.append(ddl_list)

list_schema2 = []
for l in list_schema:
     s = ' '.join(l)
     list_schema2.append(s)

create_schema_sql = """create table if not exists user_address_2018_snapshots {};"""

create_schema_sql_final = create_schema_sql.format(tuple(list_schema2)).replace("'", "")

# init Postgres connection
conn = pg.connect(database="shipping_orders",
                user="postgres",
                password="yogi200997",
                host="127.0.0.1",
                port="5432")

conn.autocommit=True
cursor=conn.cursor()

try:
    cursor.execute(create_schema_sql_final)
    print("DDL schema created succesfully..")
except pg.errors.DuplicateTable:
    print("table already creataed!")

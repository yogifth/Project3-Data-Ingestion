import json
import psycopg2 as pg
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine

schema_json = 'E:\\Yogi\\Data Engineer\\Document\\project_3_de/sql/schemas/user_address.json'
create_schema_sql = """create table if not exists user_address_2018_snapshots {};"""
database="shipping_orders"
user="postgres"
password="postgres"
host="localhost"
port="5432"
table_name = 'user_address_2018_snapshots'
zip_small_file = 'E:\\Yogi\\Data Engineer\\Document\\project_3_de/temp/dataset-small.zip'
small_file_name = 'dataset-small.csv'
result_ingestion_check_sql = 'E:\\Yogi\\Data Engineer\\Document\\project_3_de/sql/queries/result_ingestion_user_address.sql'

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

create_schema_sql_final = create_schema_sql.format(tuple(list_schema2)).replace("'", "")

# init Postgres connection
conn = pg.connect(database=database,
                user=user,
                password=password,
                host=host,
                port=port)

conn.autocommit=True
cursor=conn.cursor()

try:
    cursor.execute(create_schema_sql_final)
    print("DDL schema created succesfully..")
except pg.errors.DuplicateTable:
    print("table already creataed!")

# Load zipped file to dataframe
zf = ZipFile(zip_small_file)
df = pd.read_csv(zf.open(small_file_name), header=None)

column_name_df = [c['column_name'] for c in content]
df.columns = column_name_df

df_filtered = df[(df['created_at'] >= '2018-02-01') & (df['created_at'] <= '2018-12-31')]

# Create engine
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

# Insert to postgres
df_filtered.to_sql(table_name, engine, if_exists='replace', index=False)
print(f'Total inserted row: {len(df_filtered)}')
print(f'Initial created_at: {df_filtered.created_at.min()}')
print(f'Last created_at: {df_filtered.created_at.max()}')

cursor.execute(open(result_ingestion_check_sql, 'r').read())
result = cursor.fetchall()
print(result)
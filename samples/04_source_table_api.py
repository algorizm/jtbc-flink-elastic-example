import csv
import re
import time
from pyflink.table import (
	EnvironmentSettings, TableEnvironment, TableDescriptor
)

#########################################################
# Flink Table API - Query - Start
#########################################################
start_time = time.time()

table_env = TableEnvironment.create(
	EnvironmentSettings.in_batch_mode())
table_env.get_config().get_configuration().set_string("parallelism.default", "1")

# ANSI-SQL, ISO-8601 지원 (영문 형태의 Jul, 월 날짜는 파싱이 안됨)
source_ddl = f"""
  create table nginxlog (
    remote STRING,
	reqtime STRING
  ) with (
    'connector' = 'filesystem',
    'format' = 'csv',
    'path' = '{csv_file_name}'
  )
"""

table_env.execute_sql(source_ddl)
src = table_env.from_path("nginxlog")
table_pandas = src.to_pandas()
#########################################################
# END
#########################################################
print(f'taken time: {time.time() - start_time}')
print(f'total: {total}')

print('using pandas-filter')
dfN = table_pandas.query('remote == "211.202.132.185"').copy()
print(dfN.head())

print('using flink-sql')
table_result = table_env.sql_query("select * from nginxlog where remote = '211.202.132.185'").execute()
table_result.print()
# 221.143.144.153

# source_ddl = f"""
#   create table nginx2 (
#     remote STRING,
# 	reqtime STRING,
# 	ts AS DATE_FORMAT(reqtime, 'dd/M/yyyy:HH:mm:ss')
#   ) with (
#     'connector' = 'filesystem',
#     'format' = 'csv',
#     'path' = '{input_path}'
#   )
# """


'''
ppp_sql = t_env.sql_query("""
  SELECT total_amount / passenger_count AS price_per_person
  FROM sample_trips
""")
'''
# print(src)

# print(src.to_pandas())

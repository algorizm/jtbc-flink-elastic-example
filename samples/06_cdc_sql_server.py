from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor)
import os
from pyflink.table.expressions import col

# jars = []
# # os.path.dirname(__file__))
# for file in os.listdir(os.path.abspath('/opt/flink/lib')):
# 	if file.endswith('.jar'):
# 		jars.append(os.path.abspath(file))
# str_jars = ';'.join(['file://' + jar for jar in jars])
# print(str_jars)

# https://stackoverflow.com/questions/71470078/pyflink-14-2-table-api-ddl-semantic-exactly-once
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = TableEnvironment.create(env_settings)
#table_env.get_config().get_configuration().set_string("pipeline.jars", str_jars)

source_ddl = f'''
CREATE TABLE news_basic (
	NEWS_ID VARCHAR(10),
	NEWS_TYPE CHAR(1),
	SERVICE_DT VARCHAR(12),
	ARTICLE_TITLE VARCHAR(100),
	ARTICLE_SUMMARY VARCHAR(300),
	PRIMARY KEY (NEWS_ID) NOT ENFORCED
) WITH (
    'connector' = 'sqlserver-cdc',
    'hostname' = '192.168.172.111',
    'port' = '1433',
    'username' = 'test',
    'password' = 'test123',
    'database-name' = 'testdb',
    'schema-name' = 'dbo',
    'table-name' = 'news_basic'
)
'''

table_env.execute_sql(source_ddl)
src = table_env.from_path("news_basic")
src.select(col("NEWS_ID"), col("ARTICLE_TITLE")).execute().print()
# table_result = table_env.sql_query("select * from news_basic limit 1").execute()
# table_result.print()

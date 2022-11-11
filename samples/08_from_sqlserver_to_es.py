import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
	EnvironmentSettings, TableEnvironment, TableDescriptor
)

table_env = TableEnvironment.create(
	EnvironmentSettings.in_batch_mode())
table_env.get_config().get_configuration().set_string("parallelism.default", "1")

soure_ddl = f'''
CREATE TABLE NewsBasic (
	news_id STRING,
	news_type STRING,
	service_dt STRING,
	news_title STRING,
	news_summary STRING,
	PRIMARY KEY (news_id) NOT ENFORCED
) WITH (
	'connector' = 'jdbc',
	'url' = 'jdbc:/sqlserver://192.168.172.101:1433/DB_TEST_NEWS',
	'table-name' = 'test_news'
)
'''


'''
# TODO demo code
SET execution.checkpointing.interval = 3s;

CREATE TABLE news_basic (
	NEWS_ID VARCHAR(10),
	NEWS_TYPE CHAR(1),
	SERVICE_DT VARCHAR(12),
	ARTICLE_TITLE VARCHAR(100),
	ARTICLE_SUMMARY VARCHAR(300),
	PRIMARY KEY (NEWS_ID) NOT ENFORCED
) WITH (
    'connector' = 'sqlserver-cdc',
    'hostname' = '192.168.172.101',
    'port' = '1433',
    'username' = 'testuser',
    'password' = 'test123',
    'database-name' = 'DB_TEST',
    'schema-name' = 'dbo',
    'table-name' = 'news_basic'
);

INSERT INTO es_news_basic
SELECT NEWS_ID, NEWS_TYPE, SERVICE_DT, ARTICLE_TITLE, ARTICLE_SUMMARY FROM news_basic;

# sql-server에서 변경
select * from news_basic
where NEWS_ID = 'NB12007613'

update news_basic set ARTICLE_TITLE = '테스트 1 테스트 12'
where NEWS_ID = 'NB12007613'
'''

table_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
table_env.get_config().get_configuration().set_string("parallelism.default", "1")

src = table_env.from_path("news_basic")
result = src.select("news_id, news_title")
print(result)

'''
# specify table program
orders = table.from_path("Orders")  # schema (a, b, c, rowtime)

result = orders.filter("a.isNotNull && b.isNotNull && c.isNotNull") \
	.select("a.lowerCase() as a, b, rowtime") \
	.window(Tumble.over("1.hour").on("rowtime").alias("hourlyWindow")) \
	.group_by("hourlyWindow, a") \
	.select("a, hourlyWindow.end as hour, b.avg as avgBillingAmount")
'''

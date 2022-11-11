# https://flink-packages.org/packages/cdc-connectors
# https://ververica.github.io/flink-cdc-connectors/master/content/quickstart/sqlserver-tutorial.html
# https://github.com/ververica/flink-cdc-connectors/blob/master/docs/content/connectors/sqlserver-cdc.md
# https://ververica.github.io/flink-cdc-connectors/release-2.2/content/connectors/sqlserver-cdc.html

# netflix
# https://www.infoq.com/news/2022/04/netflix-studio-search/

'''
sudo ./bin/sql-client.sh embedded
'''


# cdc는 real-time 처리시 세션단위로 job을 실행해서 다른 시스템의 데이터를 실시간으로 참조할때 사용한다.
from pyflink.table.expressions import col

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
    'hostname' = '222.122.208.101',
    'port' = '1433',
    'username' = 'test',
    'password' = 'test123',
    'database-name' = 'testdb',
    'schema-name' = 'dbo',
    'table-name' = 'abc'
)
'''
# https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/python/faq/
from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor)

table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
# table_env.get_config().get_configuration().set_string("parallelism.default", "1")
table_env.get_config().get_configuration().set_string("pipeline.jars", "/opt/flink/lib/flink-sql-connector-sqlserver-cdc-2.2.1.jar")

table_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "10min")
table_env.get_config().get_configuration().set_string("execution.checkpointing.tolerable-failed-checkpoints", "100")
table_env.get_config().get_configuration().set_string("restart-strategy", "fixed-delay")
table_env.get_config().get_configuration().set_string("restart-strategy.fixed-delay.attempts", "2147483647")

'''
execution.checkpointing.interval: 10min
execution.checkpointing.tolerable-failed-checkpoints: 100
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 2147483647
'''
table_env.execute_sql(source_ddl)
src = table_env.from_path("news_basic")
#table_env.create_temporary_view("", src)

src.select(col("NEWS_ID"), col("ARTICLE_TITLE")).execute().print()

'''
192.168.171.226:9200

CREATE TABLE dailynews (
  news_id STRING,
  news_name STRING,
  view_total BIGINT,
  reaction_total BIGINT,
  comment_total BIGINT,
  PRIMARY KEY (news_id) NOT ENFORCED
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://192.168.171.167:9200',
  'index' = 'dailynews',
  'username' = 'elastic',
  'password'= 'QcHivgzZbL3FW7xMAcfi',
  'format' = 'json'
);

insert into dailynews values('nb123', 'test', 10, 5, 5);
'''

'''
    "hosts": "192.168.171.167:9200",
    "username": "elastic",
    "password": "QcHivgzZbL3FW7xMAcfi"
'''
from pyflink.table import TableEnvironment, EnvironmentSettings, TableDescriptor

env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
t_env = TableEnvironment.create(env_settings)
#t_env.get_config().get_configuration()\
#	.set_string("pipeline.classpath", "file:///opt/flink/lib/flink-sql-connector-elasticsearch-7_2.12-1.14.4.jar;file:///opt/flink/lib/elasticsearch-rest-high-level-client-7.13.2.jar")

sink_ddl = f"""
CREATE TABLE dailynews (
  news_id STRING,
  news_name STRING,
  view_total BIGINT,
  reaction_total BIGINT,
  comment_total BIGINT,
  PRIMARY KEY (news_id) NOT ENFORCED
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://192.168.171.167:9200',
  'index' = 'dailynews',
  'username' = 'elastic',
  'password' = 'QcHivgzZbL3FW7xMAcfi',
  'format' = 'json'
)
"""

t_env.execute_sql(sink_ddl)
t_env.execute_sql("insert into dailynews (news_id, news_name, view_total, reaction_total, comment_total) values ('NB1234567', 'test999', 2, 3, 5)")
#sink_table = t_env.execute_sql("insert into dailynews values ('NB1234567', 'test222', 2, 3, 5)")
#print(sink_table)


'''
CREATE TABLE Student (
    `user_id`   INT,
    `user_name` VARCHAR
) WITH (
	'connector' = 'elasticsearch-7',            
	'hosts' = 'http://192.168.171.167:9200',
	'index' = 'Student',        -- Elasticsearch Of Index name
	'connector.document-type' = 'stu',    -- Elasticsearch Of Document type
	'connector.username' = 'elastic',     -- Optional parameters: Please replace with actual Elasticsearch User name
	'connector.password' = 'QcHivgzZbL3FW7xMAcfi'  -- Optional parameters: Please replace with actual Elasticsearch Password
);
'''

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, Table, TableSink, TableEnvironment, EnvironmentSettings


t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
t_env.get_config().set("parallelism.default", "1")

my_source_ddl = """
    create table source (
        word STRING
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '{}'
    )
""".format(input_path)

my_sink_ddl = """
    create table sink (
        word STRING,
        `count` BIGINT
    ) with (
        'connector' = 'filesystem',
        'format' = 'canal-json',
        'path' = '{}'
    )
""".format(output_path)

t_env.execute_sql(my_source_ddl)
t_env.execute_sql(my_sink_ddl)

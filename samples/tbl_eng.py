import json
import logging
import sys

from pyflink.common import Row
from pyflink.table import (DataTypes, TableEnvironment, EnvironmentSettings)
from pyflink.table.expressions import *
from pyflink.table.udf import udtf, udf, udaf, AggregateFunction, TableAggregateFunction, udtaf


def basic_operations():
	t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

	# define the source
	table = t_env.from_elements(
		elements=[
			(1, '{"name": "Flink", "tel": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
			(2, '{"name": "hello", "tel": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
			(3, '{"name": "world", "tel": 124, "addr": {"country": "USA", "city": "NewYork"}}'),
			(4, '{"name": "PyFlink", "tel": 32, "addr": {"country": "China", "city": "Hangzhou"}}')
		],
		schema=['id', 'data'])

	right_table = t_env.from_elements(elements=[(1, 18), (2, 30), (3, 25), (4, 10)],
									  schema=['id', 'age'])

	table = table.add_columns(
		col('data').json_value('$.name', DataTypes.STRING()).alias('name'),
		col('data').json_value('$.tel', DataTypes.STRING()).alias('tel'),
		col('data').json_value('$.addr.country', DataTypes.STRING()).alias('country')) \
		.drop_columns(col('data'))
	table.execute().print()
	# +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
	# | op |                   id |                           name |                            tel |                        country |
	# +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
	# | +I |                    1 |                          Flink |                            123 |                        Germany |
	# | +I |                    2 |                          hello |                            135 |                          China |
	# | +I |                    3 |                          world |                            124 |                            USA |
	# | +I |                    4 |                        PyFlink |                             32 |                          China |
	# +----+----------------------+--------------------------------+--------------------------------+--------------------------------+

	# limit the number of outputs
	table.limit(3).execute().print()
	# +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
	# | op |                   id |                           name |                            tel |                        country |
	# +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
	# | +I |                    1 |                          Flink |                            123 |                        Germany |
	# | +I |                    2 |                          hello |                            135 |                          China |
	# | +I |                    3 |                          world |                            124 |                            USA |
	# +----+----------------------+--------------------------------+--------------------------------+--------------------------------+

	# filter
	table.filter(col('id') != 3).execute().print()
	# +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
	# | op |                   id |                           name |                            tel |                        country |
	# +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
	# | +I |                    1 |                          Flink |                            123 |                        Germany |
	# | +I |                    2 |                          hello |                            135 |                          China |
	# | +I |                    4 |                        PyFlink |                             32 |                          China |
	# +----+----------------------+--------------------------------+--------------------------------+--------------------------------+

	# aggregation
	table.group_by(col('country')) \
		.select(col('country'), col('id').count, col('tel').cast(DataTypes.BIGINT()).max) \
		.execute().print()
	# +----+--------------------------------+----------------------+----------------------+
	# | op |                        country |               EXPR$0 |               EXPR$1 |
	# +----+--------------------------------+----------------------+----------------------+
	# | +I |                        Germany |                    1 |                  123 |
	# | +I |                            USA |                    1 |                  124 |
	# | +I |                          China |                    1 |                  135 |
	# | -U |                          China |                    1 |                  135 |
	# | +U |                          China |                    2 |                  135 |
	# +----+--------------------------------+----------------------+----------------------+

	# distinct
	table.select(col('country')).distinct() \
		.execute().print()
	# +----+--------------------------------+
	# | op |                        country |
	# +----+--------------------------------+
	# | +I |                        Germany |
	# | +I |                          China |
	# | +I |                            USA |
	# +----+--------------------------------+

	# join
	# Note that it still doesn't support duplicate column names between the joined tables
	table.join(right_table.rename_columns(col('id').alias('r_id')), col('id') == col('r_id')) \
		.execute().print()
	# +----+----------------------+--------------------------------+--------------------------------+--------------------------------+----------------------+----------------------+
	# | op |                   id |                           name |                            tel |                        country |                 r_id |                  age |
	# +----+----------------------+--------------------------------+--------------------------------+--------------------------------+----------------------+----------------------+
	# | +I |                    4 |                        PyFlink |                             32 |                          China |                    4 |                   10 |
	# | +I |                    1 |                          Flink |                            123 |                        Germany |                    1 |                   18 |
	# | +I |                    2 |                          hello |                            135 |                          China |                    2 |                   30 |
	# | +I |                    3 |                          world |                            124 |                            USA |                    3 |                   25 |
	# +----+----------------------+--------------------------------+--------------------------------+--------------------------------+----------------------+----------------------+

	# join lateral
	@udtf(result_types=[DataTypes.STRING()])
	def split(r: Row):
		for s in r.name.split("i"):
			yield s

	table.join_lateral(split.alias('a')) \
		.execute().print()
	# +----+----------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
	# | op |                   id |                           name |                            tel |                        country |                              a |
	# +----+----------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
	# | +I |                    1 |                          Flink |                            123 |                        Germany |                             Fl |
	# | +I |                    1 |                          Flink |                            123 |                        Germany |                             nk |
	# | +I |                    2 |                          hello |                            135 |                          China |                          hello |
	# | +I |                    3 |                          world |                            124 |                            USA |                          world |
	# | +I |                    4 |                        PyFlink |                             32 |                          China |                           PyFl |
	# | +I |                    4 |                        PyFlink |                             32 |                          China |                             nk |
	# +----+----------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+

	# show schema
	table.print_schema()
	# (
	#   `id` BIGINT,
	#   `name` STRING,
	#   `tel` STRING,
	#   `country` STRING
	# )

	# show execute plan
	print(table.join_lateral(split.alias('a')).explain())
	# == Abstract Syntax Tree ==
	# LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{}])
	# :- LogicalProject(id=[$0], name=[JSON_VALUE($1, _UTF-16LE'$.name', FLAG(NULL), FLAG(ON EMPTY), FLAG(NULL), FLAG(ON ERROR))], tel=[JSON_VALUE($1, _UTF-16LE'$.tel', FLAG(NULL), FLAG(ON EMPTY), FLAG(NULL), FLAG(ON ERROR))], country=[JSON_VALUE($1, _UTF-16LE'$.addr.country', FLAG(NULL), FLAG(ON EMPTY), FLAG(NULL), FLAG(ON ERROR))])
	# :  +- LogicalTableScan(table=[[default_catalog, default_database, Unregistered_TableSource_249535355, source: [PythonInputFormatTableSource(id, data)]]])
	# +- LogicalTableFunctionScan(invocation=[*org.apache.flink.table.functions.python.PythonTableFunction$1f0568d1f39bef59b4c969a5d620ba46*($0, $1, $2, $3)], rowType=[RecordType(VARCHAR(2147483647) a)], elementType=[class [Ljava.lang.Object;])
	#
	# == Optimized Physical Plan ==
	# PythonCorrelate(invocation=[*org.apache.flink.table.functions.python.PythonTableFunction$1f0568d1f39bef59b4c969a5d620ba46*($0, $1, $2, $3)], correlate=[table(split(id,name,tel,country))], select=[id,name,tel,country,a], rowType=[RecordType(BIGINT id, VARCHAR(2147483647) name, VARCHAR(2147483647) tel, VARCHAR(2147483647) country, VARCHAR(2147483647) a)], joinType=[INNER])
	# +- Calc(select=[id, JSON_VALUE(data, _UTF-16LE'$.name', FLAG(NULL), FLAG(ON EMPTY), FLAG(NULL), FLAG(ON ERROR)) AS name, JSON_VALUE(data, _UTF-16LE'$.tel', FLAG(NULL), FLAG(ON EMPTY), FLAG(NULL), FLAG(ON ERROR)) AS tel, JSON_VALUE(data, _UTF-16LE'$.addr.country', FLAG(NULL), FLAG(ON EMPTY), FLAG(NULL), FLAG(ON ERROR)) AS country])
	#    +- LegacyTableSourceScan(table=[[default_catalog, default_database, Unregistered_TableSource_249535355, source: [PythonInputFormatTableSource(id, data)]]], fields=[id, data])
	#
	# == Optimized Execution Plan ==
	# PythonCorrelate(invocation=[*org.apache.flink.table.functions.python.PythonTableFunction$1f0568d1f39bef59b4c969a5d620ba46*($0, $1, $2, $3)], correlate=[table(split(id,name,tel,country))], select=[id,name,tel,country,a], rowType=[RecordType(BIGINT id, VARCHAR(2147483647) name, VARCHAR(2147483647) tel, VARCHAR(2147483647) country, VARCHAR(2147483647) a)], joinType=[INNER])
	# +- Calc(select=[id, JSON_VALUE(data, '$.name', NULL, ON EMPTY, NULL, ON ERROR) AS name, JSON_VALUE(data, '$.tel', NULL, ON EMPTY, NULL, ON ERROR) AS tel, JSON_VALUE(data, '$.addr.country', NULL, ON EMPTY, NULL, ON ERROR) AS country])
	#    +- LegacyTableSourceScan(table=[[default_catalog, default_database, Unregistered_TableSource_249535355, source: [PythonInputFormatTableSource(id, data)]]], fields=[id, data])


if __name__ == '__main__':
	logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
	basic_operations()

## Flink(docker)

#### 00. Basic flink-table
- handel table, columns, select, groupyby, schema, explain 

#### 01. flink-table udf, session, application 단위
- user-define-function, temporary-function

#### 02. handle column-level
- add, remove, rename column 

#### 03. flink-table(row, aggregate operation)
- aggregation 클래스 선언, 데이터 파싱시 계산식(연산) 메소드 적용, 테이블 변환 처리

#### 04. Transformation from nginx(csv) to pandas, flink using SQL
<u> - nginx > csv > table_api 처리, flink-sql, pandas-filter </u>
- pandas01.py, 04_source_table_api.py
- /opt/flink/bin/flink run --python 05_sink_elastic.py \
--classpath file:///opt/flink/lib/flink-sql-connector-elasticsearch7_2.12-1.14.4.jar,file:///opt/flink/lib/elasticsearch-rest-high-level-client-7.13.2.jar

#### 05. Handling Sink(elastic) Using table-api
- elastic 인덱스를 sink로 등록하고 데이터 처리하는 방법
- 05_sink_elastic.py

#### 06. Processing DataStream CDC(Java 에서만 지원) -(skip)

#### 07. How to Source(read) to Sink(write) - f/w
- Amazon S3, Google Cloud Storage, Azure Blob Storage
- 엘라, hdfs, 파일로 변환 예제 
- 엘라에 있는거 읽어와서 파일, hdfs, 등으로 가져오는 예제

#### 8. CDC from Source(SQL-Server) to Sink(Elastic)
### 08_from_sqlserver_to_es.py
filesystem, hdfs, kafka 등으로 보낼수 있음
https://ververica.github.io/flink-cdc-connectors/master/content/quickstart/sqlserver-tutorial.html

https://nightlies.apache.org/flink/flink-docs-master/docs/try-flink/table_api/
```
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
    'hostname' = '192.168.123.123',
    'port' = '1433',
    'username' = 'test',
    'password' = 'test123#$',
    'database-name' = 'DB_TEST',
    'schema-name' = 'dbo',
    'table-name' = 'news_basic'
);

CREATE TABLE es_news (
	NEWS_ID VARCHAR(10),
	NEWS_TYPE CHAR(1),
	SERVICE_DT VARCHAR(12),
	ARTICLE_TITLE VARCHAR(100),
	ARTICLE_SUMMARY VARCHAR(300),
	PRIMARY KEY (NEWS_ID) NOT ENFORCED
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://192.168.171.167:9200',
  'index' = 'test',
  'username' = 'test',
  'password' = 'test123',
  'format' = 'json'
)

INSERT INTO es_news_basic
SELECT NEWS_ID, NEWS_TYPE, SERVICE_DT, ARTICLE_TITLE, ARTICLE_SUMMARY FROM news_basic;

# sql-server에서 변경
select * from news_basic
where NEWS_ID = 'NB12007613'

update news_basic set ARTICLE_TITLE = '테스트 12345'
where NEWS_ID = 'NB12007613'
```

#### https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/python/table/intro_to_table_api/
메모리 상에서 Source(datagen)를 생성 하고 Sink(print) 넣고 벌크 데이터 저장

#### flink run, cluster 실행 방법등
https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/deployment/cli/#submitting-pyflink-jobs

#### 정책, at least once, Exactly once
https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/table/table_environment/

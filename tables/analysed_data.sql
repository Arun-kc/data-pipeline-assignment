CREATE EXTERNAL TABLE `analysed_data`(
  `id` string, 
  `post` string, 
  `comment` string, 
  `post_polarity` double, 
  `comment_polarity` double, 
  `post_sentiment` string, 
  `comment_sentiment` string, 
  `__index_level_0__` bigint)
PARTITIONED BY ( 
  `partition_0` string, 
  `partition_1` string, 
  `partition_2` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://assignment-analytics-bucket/analysed_data/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='analysed_data_crawler', 
  'averageRecordSize'='144', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='3', 
  'partition_filtering.enabled'='true', 
  'recordCount'='11080', 
  'sizeKey'='716876', 
  'typeOfData'='file')
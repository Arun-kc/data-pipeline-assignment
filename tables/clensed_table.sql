CREATE EXTERNAL TABLE `clensed_data`(
  `id` string, 
  `post` string, 
  `comment` string, 
  `interactions` struct<interaction_id:int,user1_id:int,user2_id:int,action:string,timestamp:int>, 
  `users` struct<user_id:int,user_name:string,user_age:int,user_location:string>, 
  `timestamp` timestamp)
PARTITIONED BY ( 
  `year` string, 
  `month` string, 
  `day` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://assignment-cleansed-bucket/clensed_data/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='clensed_data_crawler', 
  'averageRecordSize'='290', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='3', 
  'partition_filtering.enabled'='true', 
  'recordCount'='12', 
  'sizeKey'='13887', 
  'typeOfData'='file')
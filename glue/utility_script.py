import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import to_timestamp,regexp_replace,when,col,date_format,year,month
import boto3

def move_folder_in_s3(bucket, source_folder, destination_path):
    s3 = boto3.client('s3')
    s3_bucket = boto3.resource('s3')
    my_bucket = s3_bucket.Bucket(bucket)

    for object_summary in my_bucket.objects.filter(Prefix=source_folder):
        print(object_summary.key)
        copy_source = {'Bucket': bucket, 'Key': object_summary.key}
        destination = destination_path + object_summary.key.split(source_folder, 1)[1]
        print(destination)
        s3.copy_object(CopySource=copy_source, Bucket=bucket, Key=destination)

def delete(bucket, folder):
    s3 = boto3.client('s3')
    s3_bucket = boto3.resource('s3')
    bucket = s3_bucket.Bucket(bucket)
    bucket.objects.filter(Prefix=folder).delete()

def move_data(bucket,source_path,to_be_replaced,target):
    target_path = source_path.replace(to_be_replaced, target)
    move_folder_in_s3(bucket, source_path, target_path)
    delete(bucket, source_path)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# moving data to processing folder
move_data('assignment-landing-bucket', 'source_data/current/', 'current', 'processing')

df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://assignment-landing-bucket/source_data/processing/"],
        "recurse": True,
    }
)
df = df.toDF()

#removing special characters
df = df.withColumn("post",regexp_replace("post", "[^0-9a-zA-Z_,\-@.\s]+", ""))\
            .withColumn("comment",regexp_replace("comment", "[^0-9a-zA-Z_,\-@.\s]+", ""))\
            .withColumn("timestamp",to_timestamp("timestamp"))
     
#replacing empty cells with NULL value
df = df.withColumn("id",when(col('id')=="", None).otherwise(col('id')))\
        .withColumn("post",when(col('post')=="", None).otherwise(col('post')))\
        .withColumn("comment",when(col('comment')=="", None).otherwise(col('comment')))

#column creation for partition
df = df.withColumn("year",year(df.timestamp))\
        .withColumn("month",month(df.timestamp))\
        .withColumn("day",date_format(col("timestamp"), "d"))

df = df.repartition(1)

final_df = DynamicFrame.fromDF(df, glueContext, "dataframe")

final_df.toDF().show()

#writing data to redshift table
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=final_df, 
    catalog_connection='RedshiftConn', 
    connection_options={
        "dbtable": "dw.clensed_data_tbl",
        "database": "dwd"
    },
    redshift_tmp_dir=args["TempDir"]
)

#loading data to s3
glueContext.write_dynamic_frame.from_options(
    frame=final_df,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://assignment-cleansed-bucket/social_media",
        "partitionKeys": ["year", "month", "day"],
    }
)

#moving data to archive
move_data('assignment-landing-bucket', 'source_data/processing/', 'processing', 'archive')

job.commit()
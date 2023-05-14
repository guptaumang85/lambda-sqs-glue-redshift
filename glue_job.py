import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import boto3
import json

def poll_message_from_sqs(queue_url):
  sqs = boto3.client('sqs')
  messages = []
  while True:
    response = sqs.receive_message(
      QueueUrl= queue_url,
      AttributeNames=['All'],
      MaxNumberOfMessages=10,
      WaitTimeSeconds=10
      )
    if 'Messages' in response:
      messages.extend(response['Message'])
    else:
      break

  return [json.loads(msg['Body']) for msg in messages]

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

#poll messages from sqs
queue_url = "https://sqs.ap-south-1.amazonaws.com/370876035393/demo-sqs"
messages = poll_message_from_sqs(queue_url)

json_str = json.dumps(messages)

emp_spark_df = spark.read.json(sc.parallelize([json_str]))

emp_spark_df.show()

emp_df = DynamicFrame.frameDF(glueContext.createDataFrame(records), glueContext, "records")

emp_df.printSchema()
emp_df.show()

#write data into redshift table
sink = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = emp_df,
    catalog_connection = "redshift-connection",
    connection_url = "jdbc:redshift://redshift-connection.cyj2i4okk4dl.ap-south-1.redshift.amazonaws.com:5439/dev",
    connection_properties = {
      "user": "admin",
      "password": {REDSHIFT_PASSWORD}
    },
    table_name="public.employee",
    redshift_tmp_dir="s3://demo-data-employee/tmp_redshift_data/",
    transformation_ctx="sink"
  )

print("redshift ingestion successful")

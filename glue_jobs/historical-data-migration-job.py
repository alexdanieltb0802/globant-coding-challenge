import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import boto3
import json

# Glue Context
sc=SparkContext.getOrCreate()
sc.setLogLevel("ERROR")
glueContext = GlueContext(sc)
spark_session = glueContext.spark_session
sqlContext = SQLContext(spark_session.sparkContext, spark_session)

print ("starting job********************* \n")

secrets_client = boto3.client('secretsmanager')
secrets_response = secrets_client.get_secret_value(SecretId='secret-globant-challenge')
secret = json.loads(secrets_response['SecretString'])
host = secret['host']
user = secret['username']
passwd = secret['password']
port = secret['port']
engine = secret['engine']
# driver = secret['driver']
database = 'globant'

departments_schema = StructType([ \
    StructField("id", IntegerType(), False), \
    StructField("department", StringType(), False) \
  ])

df_departments = sqlContext.read.format('csv') \
    .options(header='false', sep=',') \
    .schema(departments_schema) \
    .load('s3://globant-source-bucket/source-tables/departments.csv')

jobs_schema = StructType([ \
    StructField("id", IntegerType(), False), \
    StructField("job", StringType(), False) \
  ])

df_jobs = sqlContext.read.format('csv') \
    .options(header='false', sep=',') \
    .schema(jobs_schema) \
    .load('s3://globant-source-bucket/source-tables/jobs.csv')

hired_employees_schema = StructType([ \
    StructField("id", IntegerType(), False), \
    StructField("name", StringType(), True), \
    StructField("datetime", StringType(), True), \
    StructField("department_id", IntegerType(), True), \
    StructField("job_id", IntegerType(), True) \
  ])

df_hired_employees = sqlContext.read.format('csv') \
    .options(header='false', sep=',') \
    .schema(hired_employees_schema) \
    .load('s3://globant-source-bucket/source-tables/hired_employees.csv')    
    

print ("\nRows get from Query: {0} \n".format(df_departments.count()))
df_departments.printSchema()
print ("\nRows get from Query: {0} \n".format(df_jobs.count()))
df_jobs.printSchema()
print ("\nRows get from Query: {0} \n".format(df_hired_employees.count()))
df_hired_employees.printSchema()


print('----Start Saving')
# # Rules mysql Connection Options
mysql_options_jobs = {
    "url": f"jdbc:{engine}://{host}:{port}/{database}",
    "dbtable": "jobs",
    "user": user,
    "password": passwd
    }
mysql_options_departments = {
    "url": f"jdbc:{engine}://{host}:{port}/{database}",
    "dbtable": "departments",
    "user": user,
    "password": passwd
    }
mysql_options_hired_employees = {
    "url": f"jdbc:{engine}://{host}:{port}/{database}",
    "dbtable": "hired_employees",
    "user": user,
    "password": passwd
    }

result_df_jobs_dyf = DynamicFrame.fromDF(df_jobs, glueContext, "result_dyf")
result_df_departments_dyf = DynamicFrame.fromDF(df_departments, glueContext, "result_dyf")
result_df_hired_employees_dyf = DynamicFrame.fromDF(df_hired_employees, glueContext, "result_dyf")

glueContext.write_from_options(frame_or_dfc=result_df_jobs_dyf, connection_type="mysql",
                              connection_options=mysql_options_jobs)
glueContext.write_from_options(frame_or_dfc=result_df_departments_dyf, connection_type="mysql",
                              connection_options=mysql_options_departments)
glueContext.write_from_options(frame_or_dfc=result_df_hired_employees_dyf, connection_type="mysql",
                              connection_options=mysql_options_hired_employees)


print ("OK********************* \n")
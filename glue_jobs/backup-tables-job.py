import sys
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SQLContext
import boto3
import json

# Glue Context
sc=SparkContext.getOrCreate()
sc.setLogLevel("ERROR")
glueContext = GlueContext(sc)
spark_session = glueContext.spark_session
sqlContext = SQLContext(spark_session.sparkContext, spark_session)

def write_df_toS3(spark_df,prefix,s3_dest_bucket,output_table):
    ###################################
    # Saving table in S3 (Parquet, compression default, partitions by ANNO ADUANA)
    ###################################
    result_dyf = DynamicFrame.fromDF(spark_df, glueContext, "result_dyf")
    
    s3_final_path = s3_dest_bucket + output_table
    print(s3_final_path)
    glueContext.write_dynamic_frame.from_options(frame = result_dyf,
                                                    connection_type = "s3",
                                                    connection_options = {"path": s3_final_path},
                                                    format = "avro")

print ("starting job********************* \n")

secrets_client = boto3.client('secretsmanager')
secrets_response = secrets_client.get_secret_value(SecretId='secret-globant-challenge')
secret = json.loads(secrets_response['SecretString'])
host = secret['host']
user = secret['username']
passwd = secret['password']
port = secret['port']
dbInstanceIdentifier = secret['dbInstanceIdentifier']
engine = secret['engine']
# driver = secret['driver']
database = 'globant'
s3_dest_bucket= "s3://globant-source-bucket/backup_tables/"

table_name_departments = "departments"
table_name_jobs = "jobs"
table_name_hired_employees = "hired_employees"

departments_query = f"(select * from {database}.{table_name_departments} ) as TMP"

# Get Data
spark_df_departments = sqlContext.read.format("jdbc") \
.option("url", f"jdbc:{engine}://{host}:{port}/{database}") \
.option("user", user) \
.option("password", passwd) \
.option("dbtable", departments_query) \
.load()

print ("\nRows get from Query: {0} \n".format(spark_df_departments.count()))
spark_df_departments.printSchema()

jobs_query = f"(select * from {database}.{table_name_jobs} ) as TMP"

# Get Data
spark_df_jobs = sqlContext.read.format("jdbc") \
.option("url", f"jdbc:{engine}://{host}:{port}/{database}") \
.option("user", user) \
.option("password", passwd) \
.option("dbtable", jobs_query) \
.load()

print ("\nRows get from Query: {0} \n".format(spark_df_jobs.count()))
spark_df_jobs.printSchema()

hired_employees_query = f"(select * from {database}.{table_name_hired_employees} ) as TMP"

# Get Data
spark_df_hired_employees = sqlContext.read.format("jdbc") \
.option("url", f"jdbc:{engine}://{host}:{port}/{database}") \
.option("user", user) \
.option("password", passwd) \
.option("dbtable", hired_employees_query) \
.load()

print ("\nRows get from Query: {0} \n".format(spark_df_hired_employees.count()))
spark_df_hired_employees.printSchema()



print ("OK********************* \n")
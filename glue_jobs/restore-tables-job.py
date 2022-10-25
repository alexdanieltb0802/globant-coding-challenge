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
import logging
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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

df_departments = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://globant-source-bucket/backup_tables/departments/run-1666643294864-part-r-00000"]},
    format="avro"
)

df_jobs = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://globant-source-bucket/backup_tables/jobs/run-1666643301384-part-r-00000"]},
    format="avro"
)

df_hired_employees = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://globant-source-bucket/backup_tables/jobs/run-1666643301384-part-r-00000"]},
    format="avro"
)

print ("\nRows get from Query: {0} \n".format(df_departments.count()))
df_departments.printSchema()
print ("\nRows get from Query: {0} \n".format(df_jobs.count()))
df_jobs.printSchema()
print ("\nRows get from Query: {0} \n".format(df_hired_employees.count()))
df_hired_employees.printSchema()

try:
    conn = pymysql.connect(host=host, user=user, passwd=passwd, db=database, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

# truncate_query = """USE globant
# SET FOREIGN_KEY_CHECKS = 0
# TRUNCATE table departments
# TRUNCATE table jobs
# SET FOREIGN_KEY_CHECKS = 1

# SET FOREIGN_KEY_CHECKS = 0
# TRUNCATE table hired_employees
# SET FOREIGN_KEY_CHECKS = 1
# ;"""

# print(truncate_query)

# with conn.cursor() as cur:
#     cur.execute(truncate_query)
# conn.commit()

print('----Start Saving')
# Rules mysql Connection Options
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

glueContext.write_from_options(frame_or_dfc=df_jobs, connection_type="mysql",
                              connection_options=mysql_options_jobs)
glueContext.write_from_options(frame_or_dfc=df_departments, connection_type="mysql",
                              connection_options=mysql_options_departments)
glueContext.write_from_options(frame_or_dfc=df_hired_employees, connection_type="mysql",
                              connection_options=mysql_options_hired_employees)


print ("OK********************* \n")
import json
import sys
import logging
import rds_config
import pymysql

rds_host  = "dbglobant.cifcchy9sefj.us-east-1.rds.amazonaws.com"
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name


logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")
def lambda_handler(event, context):
    ##Insertamos data de departments:
    if 'departments' in event:
        query_departments = "INSERT INTO departments(id,department) VALUES "
        for deparment in event['departments']:
            query_departments+=f"({deparment['id']},'{deparment['department']}'),"
        query_departments = query_departments[:-1]
        with conn.cursor() as cur:
            cur.execute(query_departments)
        conn.commit()
    ##Insertamos data de jobs
    if 'jobs' in event:
        query_jobs = "INSERT INTO jobs(id,job) VALUES "
        for job in event['jobs']:
            query_jobs+=f"({job['id']},'{job['job']}'),"
        query_jobs = query_jobs[:-1]
        with conn.cursor() as cur:
            cur.execute(query_jobs)
        conn.commit()
    ##Insertamos data de hired_employees
    if 'hired_employees' in event:
        query_employee = "INSERT INTO hired_employees(id,name,datetime,department_id,job_id) VALUES "
        for employee in event['hired_employees']:
            query_employee+=f"({employee['id']},'{employee['name']}','{employee['datetime']}',{employee['department_id']},{employee['job_id']}),"
        query_employee = query_employee[:-1]
        with conn.cursor() as cur:
            cur.execute(query_employee)
        conn.commit()
    return {
        'statusCode': 200,
        'body': {
            'inserted_departments': f"inserted {len(event['departments']) if 'departments' in event else 0} elements",
            'inserted_employees': f"inserted {len(event['hired_employees']) if 'hired_employees' in event else 0} elements",
            'inserted_jobs': f"inserted {len(event['jobs']) if 'jobs' in event else 0} elements",
            }
    }

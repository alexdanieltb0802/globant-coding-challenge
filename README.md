This repository is focused on solving the proposed challenges and storing the required extraction, transformation and loading files. It also contains the complementary files for this purpose.

The main structure is as follows:
1. glue_jobs:
  - historical-data-migration-job.py: Historical load script to perform the extraction task from the S3 source and the data load to the Mysql RDS destination
  - backup-tables-job.py: get data from the database and store a backup of each table in Avro format at S3 buckets
  - Table restore script

2. lambda_function: POST data to https://rn44gc3d99.execute-api.us-east-1.amazonaws.com/dev
  - lambda_function.py: function in charge of hosting the REST service for frequent data loading.
  - test_event.json : json file example

3. Sql:
  - Database generation script
  - challenge2_req1.sql: sql script for challenge 2 - requirement 1
  - challenge2_req2.sql: sql script for challenge 2 - requirement 2

4. res: Folder for all static resources in the project. For example, dashboards, images, etc.
  - Quicksight graphic for challenge 2 - requirement 1
  - Quicksight graphic for challenge 2 - requirement 2

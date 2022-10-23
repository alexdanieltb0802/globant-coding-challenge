This repository is focused on solving the proposed challenges and storing the required extraction, transformation and loading files. It also contains the complementary files for this purpose.

The main structure is as follows:
-Glue_jobs:
  -- Historical load script: performs the extraction task from the S3 source and the data load to the Mysql RDS destination
  -- Table backup script
  -- Table restore script
-Lambda_function:
  -- function in charge of hosting the REST service for frequent data loading.
-Sql
  -- Database generation script
  -- View 1: challenge 2, requirement 1
  -- View 2: challenge 2, requirement 2

This repository is focused on solving the proposed challenges and storing the required extraction, transformation and loading files. It also contains the complementary files for this purpose.

The main structure is as follows:
1. glue_jobs:
  - Historical load script: performs the extraction task from the S3 source and the data load to the Mysql RDS destination
  - Table backup script
  - Table restore script

2. lambda_function:
  - function in charge of hosting the REST service for frequent data loading.
  - POST data to https://rn44gc3d99.execute-api.us-east-1.amazonaws.com/dev
  - test_event.json : json file example

3. Sql:
  - Database generation script
  - View 1: challenge 2, requirement 1
  - View 2: challenge 2, requirement 2

4. res: Folder for all static resources in the project. For example, dashboards, images, etc.
  - Quicksight graphic for challenge 2, requirement 1
  - Quicksight graphic for challenge 2, requirement 2

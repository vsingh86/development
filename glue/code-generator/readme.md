## Auto Generate Code 

The purpose of this program is to auto generate ETL code including:
  - Postgres ETL Views in etl_configuration schema 
  - Postgres Migration scripts 
  - Postgres Grant scripts
  - Snowflake source data tables in data_ingestion database 
  - Snowflake Dynamic tables 
  - Snowflake Build script
  - Snowflake Grant script
  - Cloudformation 


There are two approaches to generate code:

Option 1 - Configuration 
- This approach takes the tables defined in configuration file and only generates code for those objects. 


Option 2 - Schema
The objects are created for all the tables within a schema. You can specify multiple schemas
- Load method is determined based on the following :
    If the table has a column named "created_at" it will assign "incremental" as load method
    If the table has a column named "updated_at" it will assign "merge" as load method (this overwrites the above logic if the table has both created_at and updated_at)
    If table has no created_at or updated_at it will assign "full" as load method



## Migration Scripts

- After you review the the changes and commit your changes, run the following command to combine ETL views changes into a single file that can be used in migration script

```
git show --name-only --pretty="" HEAD | tail -n +2 | xargs cat > combined.txt
```

- Clean up combined.txt file. Remove commented select statements

- Create an empty migration script and copy & paste code from combined.txt into the new migration script. 


Issues:
- Handle dynamic table script creation if target schema or target database is blank. This information is stored at schema level, therefore it is set when 
source database and schema is defined. 
- Try to have disctinct tables for a database in config file and add columns for client. A flag can be set for dynamic tables to build union statement. 
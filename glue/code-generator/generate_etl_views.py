import configparser
import os
from typing import List
import psycopg2
from psycopg2 import sql
from sshtunnel import SSHTunnelForwarder
import csv 
import psycopg2.extensions
from config_loader import AppConfig, load_config


class MappingConfig:
  def __init__(self, client_name: str, source_database: str, source_schema: str, source_table: str, source_etl_view: str, load_method: str, load: str = "y", description: str = ""):
    self.client_name: str = client_name
    self.source_database: str = source_database
    self.source_schema: str = source_schema
    self.source_table: str = source_table
    self.source_etl_view: str = source_etl_view
    self.load_method: str = load_method
    self.load: str = load
    self.description: str = description
    self.target: List[Target] = []

class Target:
  def __init__(self, target_database: str,target_schema: str,target_table: str):
    self.target_database: str = target_database
    self.target_schema: str = target_schema
    self.target_table: str = target_table

class Column:
  def __init__(self, name: str, data_type: str, numeric_precision: int = None, numeric_scale: int = None, udt_name = "", sf_data_type: str = ""):
    self.name: str = name
    self.data_type: str = data_type
    self.numeric_precision: int = numeric_precision
    self.numeric_scale: int = numeric_scale
    self.udt_name: str = udt_name
    self.sf_data_type: str = sf_data_type
    
    if self.data_type.startswith("timestamp"):
      self.sf_data_type = "timestamp"
    elif self.data_type == "numeric":
      self.sf_data_type = f"numeric ({self.numeric_precision}, {self.numeric_scale})"
    else:
      self.sf_data_type = 'text'
      
class Table:
  def __init__(self, name: str):
    self.name: str = name
    self.load_method: str = ""
    self.columns: List[Column] = []
    self.target_table: str = ""
    self.load_source_data_clients = [] 
    self.dynamic_table_clients = [] 

  def add_column(self, column: Column) -> None:
    self.columns.append(column)

class Schema:
  def __init__(self, database: str, name: str, target_database: str, target_schema: str, load_source_data_clients = [], dynamic_table_clients = [] ):
    self.name: str = name
    self.database: str = database
    self.target_schema: str = target_schema
    self.target_database: str = target_database
    self.tables: List[Table] = []
    self.load_source_data_clients = load_source_data_clients
    self.dynamic_table_clients = dynamic_table_clients 
  
  def __str__(self):
        return f"{self.name}"

  def add_table(self, table: Table) -> None:
    self.tables.append(table)

  
config = load_config('glue\\code-generator\\config.ini')

def find_or_create_schema(schemas: List[Schema], database: str, schema_name: str, target_database: str, target_schema: str, load_source_data_clients = [], dynamic_table_clients = []) -> Schema:
    for schema in schemas:
        if schema.name == schema_name and schema.database == database:
            return schema
    # If not found, create a new Schema and add it to the list
    new_schema = Schema(database, schema_name, target_database, target_schema, load_source_data_clients, dynamic_table_clients)
    schemas.append(new_schema)
    return new_schema


def create_db_connection(environment):
  tunnel = None
  conn = None
  if (environment == "local"):
    # Establish the database connection
    conn = psycopg2.connect(
        dbname=config.postgresql.db_name,
        user=config.postgresql.db_user,
        password=config.postgresql.db_password,
        host=config.postgresql.db_host,
        port=config.postgresql.db_port
    )
  else: 
    # Initialize the SSH tunnel
    tunnel = SSHTunnelForwarder(
        (config.postgresql.ssh_host, 22),
        ssh_username=config.postgresql.ssh_username,
        ssh_pkey=config.postgresql.ssh_private_key,
        remote_bind_address=(config.postgresql.db_host, int(config.postgresql.db_port)),
        local_bind_address=('0.0.0.0', 10022),
        set_keepalive=120
    )
    # Start the SSH tunnel
    tunnel.start()

    # Establish the database connection
    conn = psycopg2.connect(
        dbname=config.postgresql.db_name,
        user=config.postgresql.db_user,
        password=config.postgresql.db_password,
        host='localhost',
        port=tunnel.local_bind_port  # Use the dynamically assigned local port
    )
  return tunnel, conn


  
def get_load_method (columns):
  created_at_exists = any(col[0] == 'created_at' for col in columns)
  updated_at_exists = any(col[0] == 'updated_at' for col in columns)
  load_method = "full"
  if created_at_exists: 
    load_method = "incremental"
  if updated_at_exists:
    load_method = "merge"
  return load_method

# def get_schema_data(config: AppConfig, pg_database, environment): 
#   schema_names = config.load_data.schema_names.split(',')
#   schemas: List[Schema] = []
#   tunnel, conn = create_db_connection(environment)
  
#   try:
#     with conn.cursor() as cursor:
#       for schema in schema_names:
#         # Execute the query to get all tables in the schema
#         query = sql.SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = %s")
#         cursor.execute(query, [schema])
#         tables = cursor.fetchall()
#         schema = Schema(pg_database, schema)
#         for (table_name,) in tables:
#           table = Table(table_name)
#           cursor.execute(sql.SQL(
#               "SELECT column_name, data_type, numeric_precision, numeric_scale, udt_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s"),
#               [schema.name, table_name]
#           )
#           columns = cursor.fetchall()
#           # Loop through columns and add details to the table_info
#           for column in columns:
#             column_obj = Column(*column)
#             table.add_column(column_obj)
          
#           table.load_method = get_load_method(columns)
#           # Append the table info to the schema_data list
#           schema.add_table(table)
#         schemas.append(schema)
#   except psycopg2.Error as e:
#     print(f"Database query failed: {e.pgerror}")
#     print(f"Detailed error: {e.diag.message_detail}")
#   finally:
#     conn.close()
#     tunnel.stop()
#   return schemas
    
    
def get_schema_data_config(pg_database, environment): 
  csv_file_path = f"{config.outputdir.pg_sf_mapping_config_path}\\pg_to_sf_mapping_revamped.csv"  
  schema_list: List[Schema] = []
  tunnel, conn = create_db_connection(environment)
  with open(csv_file_path, mode='r', newline='') as file:
    reader = csv.DictReader(file)  # Assuming the CSV has headers
    try:
      with conn.cursor() as cursor:
        for row in reader:
          # Define the columns you want to use for distinctness
          if row['source_database'] == pg_database and row['generate_code'] == 'y':
            schema_name = row['source_schema']
            target_schema = row['target_schema']
            target_database = row['target_database']
            load_source_data_clients = row['load_source_data_clients'].split('|')
            dynamic_table_clients = row['dynamic_table_clients'].split('|')
            schema: Schema = find_or_create_schema(schema_list, pg_database, schema_name, target_database, target_schema, load_source_data_clients, dynamic_table_clients)
            
            if row['source_table'] == "*":
              cursor.execute(sql.SQL(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = %s"),
                [schema_name]
              )
              tables = cursor.fetchall()
              for tbl in tables:
                table = schema_data_table_columns(schema.name, tbl[0], cursor, load_source_data_clients, dynamic_table_clients)                
                schema.add_table(table)
            else:
              table_name = row['source_table']          
              table = schema_data_table_columns(schema.name, table_name, cursor, load_source_data_clients, dynamic_table_clients)
              schema.add_table(table)
            
    except psycopg2.Error as e:
      print(f"Database query failed: {e.pgerror}")
      print(f"Detailed error: {e.diag.message_detail}")
    finally:
      conn.close()
      if tunnel != None:
        tunnel.stop()
  return schema_list       


def schema_data_table_columns(schema_name: str, table_name: str, cursor, load_source_data_clients, dynamic_table_clients):
  table = Table(table_name)
  table.dynamic_table_clients = dynamic_table_clients
  table.load_source_data_clients = load_source_data_clients
  cursor.execute(sql.SQL(
      "SELECT column_name, data_type, numeric_precision, numeric_scale, udt_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s"),
      [schema_name, table_name]
  )
  columns = cursor.fetchall()
  # Loop through columns and add details to the table_info
  for column in columns:
    column_obj = Column(*column)
    table.add_column(column_obj)
  load_method = get_load_method(columns)
  table.load_method = load_method
  return table


def generate_db_objects_using_config(environment, pg_database):
  schema_data = get_schema_data_config(pg_database, environment)
  
  # for schema in schema_data:
  #   print (f"Schema: {schema.name}")
  #   for table in schema.tables:
  #     print (f"Table: {table.name}, load_source_clients: {table.load_source_data_clients}, dynamic_table_clients: {table.dynamic_table_clients}")
      
  
  # generate_pg_objects(schema_data)
  # generate_sf_tables(schema_data)
  # generate_sf_dynamic_tables(schema_data)
  generate_sf_build_script(schema_data)
    
# def generate_db_objects_using_schemas(environment, pg_database, sf_dynamic_table_db, sf_dynamic_table_schema):
#   schema_data = get_schema_data(pg_database, environment)
#   generate_pg_objects(schema_data)
#   generate_sf_tables(schema_data)
#   generate_sf_dynamic_tables(schema_data)
#   generate_sf_build_script(schema_data)
#   # generate_config_csv(schema_data, sf_dynamic_table_db, sf_dynamic_table_schema)

def generate_pg_objects(schema_list: List[Schema]):
  os.makedirs(config.outputdir.pg_etl_views_path, exist_ok=True)  
  os.makedirs(config.outputdir.pg_migration_script_path, exist_ok=True)
  os.makedirs(config.outputdir.pg_grant_script_path, exist_ok=True)
  
  pg_migration_script = ""
  pg_grant_script = ""

  for schema in schema_list:
    print (f"Generting postgres ETL views for schema: {schema}")
    for table in schema.tables:
      # columns_list = ', '.join([f"\n\t\tt.{col.name}" for col in table.columns])
      
      columns_list = ', '.join([
        f"\n\t\tto_jsonb(t.{col.name}) as {col.name}"  if col.udt_name.lower() == "_metric_value" else
        f"\n\t\tCASE WHEN t.dob > '2100-01-01' THEN '1900-01-01'::date ELSE dob END as dob"  if col.name == "dob" else
        f"\n\t\tt.{col.name}"
        for col in table.columns
      ])

      join_clause = ""
      if table.load_method == 'incremental': 
        join_clause = f"\n\tLEFT JOIN etl_configuration.data_processing_status s ON ('{schema.name}' = s.schema_name and '{table.name}' = s.table_name) \n\tWHERE \n\t\ts.last_processed_timestamp is null or t.created_at > s.last_processed_timestamp"
      
      if table.load_method == 'merge':
        join_clause = f"\n\tLEFT JOIN etl_configuration.data_processing_status s ON ('{schema.name}' = s.schema_name and '{table.name}' = s.table_name) \n\tWHERE \n\t\ts.last_processed_timestamp is null or t.updated_at > s.last_processed_timestamp"

      view_name = f"{schema.name}_{table.name}_vw"
      view_sql = f"DROP VIEW IF EXISTS etl_configuration.{view_name};\nCREATE OR REPLACE VIEW etl_configuration.{view_name} \nAS \n\tSELECT {columns_list} \n\tFROM {schema.name}.{table.name} t{join_clause};"
      view_sql_with_select = f"{view_sql} \n\n-- SELECT * FROM etl_configuration.{view_name};"
      pg_migration_script = f"{pg_migration_script} \n\n{view_sql}"
      
      grant_sql = f"grant select on etl_configuration.{view_name} to etl_role;"
      pg_grant_script = f"{pg_grant_script} \n{grant_sql}"

      file_path = os.path.join(config.outputdir.pg_etl_views_path, f"{view_name}.sql")
      with open(file_path, "w") as file:
        file.write(view_sql_with_select)
      
    #Generate Postgres Migration Script  
    migration_file_path = os.path.join(config.outputdir.pg_migration_script_path, "migration.sql")
    with open(migration_file_path, "w") as migration_file:
      migration_file.write(pg_migration_script.upper())
      
    #Generate Postgres Grant Script  
    grant_file_path = os.path.join(config.outputdir.pg_grant_script_path, "grant.sql")
    with open(grant_file_path, "w") as grant_file:
      grant_file.write(pg_grant_script.upper())
    
def generate_sf_tables(schema_list: List[Schema]):
  for schema in schema_list:     
    for table in schema.tables:
      for client in table.load_source_data_clients:   
        sf_table_output_dir = f"{config.outputdir.sf_staging_tables_path}\\data_ingestion\\schemas\\greyhavens_{client}_{schema.name}\\tables"  
        os.makedirs(sf_table_output_dir, exist_ok=True)  # Create the directory if it doesn't exist
        columns_list = ', '.join([f"\n\t{col.name} {col.sf_data_type}"for col in table.columns])
        sf_table_name = f"data_ingestion.greyhavens_{client}_{schema.name}.{table.name}"
        sf_stage_table_name = f"data_ingestion.greyhavens_{client}_{schema.name}.stage_{table.name}"
        table_drop_statement = f"DROP TABLE IF EXISTS {sf_table_name};"
        table_create_statement = f"CREATE TABLE {sf_table_name} ( {columns_list}\n);"
        table_select_statement = f"-- SELECT * FROM {sf_table_name} LIMIT 5;"
        table_final_script  = f"{table_drop_statement}\n{table_create_statement}\n{table_select_statement}"
        # Generate Create Table Script
        file_path = os.path.join(sf_table_output_dir, f"{table.name}.sql")
        with open(file_path, "w") as file:
          file.write(table_final_script.upper())
          
        #Generate Create table script for stage table
        if table.load_method == 'merge':
          stage_table_sql = f"DROP TABLE IF EXISTS {sf_stage_table_name};\nCREATE TABLE {sf_stage_table_name} ( {columns_list}\n);"
          file_path = os.path.join(sf_table_output_dir, f"stage_{table.name}.sql")
          with open(file_path, "w") as file:
            file.write(stage_table_sql.upper())
    
def generate_sf_dynamic_tables(scheme_list: List[Schema]):
  for schema in scheme_list:
    for table in schema.tables:
      sf_dynamic_table_output_dir = f"{config.outputdir.sf_dynamic_tables_path}\\{schema.target_database}\\schemas\\{schema.target_schema}\\tables"  
      os.makedirs(sf_dynamic_table_output_dir, exist_ok=True)  # Create the directory if it doesn't exist
      columns_list = ', '.join([
        f"\n\t\tto_binary(replace({col.name},'-', '')) as {col.name}"  if col.data_type == "uuid" else
        f"\n\t\t{col.name}"
        for col in table.columns
      ])
      
      dynamic_table_name = f"{schema.target_database}.{schema.target_schema}.{table.name}"
      dynamic_table_sql_drop = f"DROP DYNAMIC TABLE IF EXISTS {dynamic_table_name}"
      dynamic_table_sql_create = f"CREATE DYNAMIC TABLE IF NOT EXISTS {dynamic_table_name} \nTARGET_LAG = 'DOWNSTREAM' \nWAREHOUSE = ETL_WH \nAS"
      dynamic_table_sql_select = ""
      file_path = os.path.join(sf_dynamic_table_output_dir, f"{table.name}.sql")
      for idx, client in enumerate(table.dynamic_table_clients):
        staging_table_name = f"data_ingestion.greyhavens_{client}_{schema.name}.{table.name}"
        if idx > 0:
          dynamic_table_sql_select = f"{dynamic_table_sql_select}\n\tUNION"
        dynamic_table_sql_select = f"{dynamic_table_sql_select}\n\tSELECT {columns_list} \n\tFROM {staging_table_name}"
      
      dynamic_table_sql_select_test = f"\n-- SELECT * FROM {dynamic_table_name} LIMIT 5"
      dynamic_table_sql_final = f"{dynamic_table_sql_drop};\n{dynamic_table_sql_create}{dynamic_table_sql_select};\n{dynamic_table_sql_select_test};"
      with open(file_path, "w") as file:
        file.write(dynamic_table_sql_final.upper())

def generate_sf_build_script(schema_list: List[Schema]):
    clients = config.load_data.client_names.split(',')
    sf_build_script_dir = config.outputdir.sf_build_script_path
    os.makedirs(sf_build_script_dir, exist_ok=True)  # Create the directory if it doesn't exist
    build_script_file_path = os.path.join(sf_build_script_dir, "build.sql")
    build_script = []
    for schema in schema_list:
      for client in schema.load_source_data_clients:
        build_script.append(f"-f data_ingestion\\schemas\\{schema.database}_{client}_{schema.name}\\{schema.database}_{client}_{schema.name}.sql `\n")
        for table in schema.tables:
          build_script.append(f"-f data_ingestion\\schemas\\{schema.database}_{client}_{schema.name}\\tables\\{table.name}.sql `\n")
          if (table.load_method == "merge"):
            build_script.append(f"-f data_ingestion\\schemas\\{schema.database}_{client}_{schema.name}\\tables\\stage_{table.name}.sql `\n")
    
    # Dynamic Tables
    for schema in schema_list:
      for table in schema.tables:
        if len(table.dynamic_table_clients) > 0:
          build_script.append(f"-f {schema.target_database}\\schemas\\{schema.target_schema}\\tables\\{table.name}.sql `\n")  
            
    with open(build_script_file_path, "w") as file:
      file.writelines(build_script)
      

# def generate_config_json(schema_data: List[Schema], sf_db_name, sf_schema_name):
#   # Directory where the SQL files will be saved
#   conif_file_dir = config.outputdir.pg_sf_mapping_config_path
#   os.makedirs(conif_file_dir, exist_ok=True)  # Create the directory if it doesn't exist
#   clients = config.load_data.client_names.split(',')
#   # Replace these with your server's details
  
#   mongo_uri = config('mongodb').get('mongo_uri')
#   database_name = config('mongodb').get('database_name')
  
#   print (f"mongo_uri: {mongo_uri}")
#   client = MongoClient(mongo_uri)
#   db = client[database_name]
#   collection = db['config']
  
#   # #Generate Configuration File
#   config_script = []
#   for client in clients:    
#     for schema in schema_data:
#       for table in schema.tables:
#         pg_view_name = f"{schema.name}_{table.name}_vw"
#         config_element = MappingConfig(client,schema.database, schema.name, table.name, pg_view_name, table.load_method)
#         config_element.target = Target(sf_db_name, sf_schema_name, table.name)
#         config_data = {
#           '$set': {
#             'client_name': config_element.client_name,
#             'source_database': config_element.source_database,
#             'source_schema': config_element.source_schema,
#             'source_table': config_element.source_table,
#             'source_etl_view': config_element.source_etl_view,
#             'load_method': config_element.load_method,
#             'load': config_element.load,
#             'description': config_element.description,
#             'target': [{
#               'target_database': config_element.target.target_database,
#               'target_schema': config_element.target.target_schema,
#               'target_table': config_element.target.target_table
#             }]
#           }
#         }
#         # Upsert based on composite key
#         result = collection.update_one(
#             {'client_name':  config_element.client_name, 'source_database': config_element.source_database,"source_schema": config_element.source_schema, "source_table": config_element.source_table },  # Composite key filter
#             config_data,
#             upsert=True  # Enable upsert
#         )
#         if result.matched_count:
#             print("Document updated.")
#         elif result.upserted_id:
#             print(f"New document inserted with _id: {result.upserted_id}")
#         else:
#             print("No changes made.")
      

# def generate_config_csv(schema_data: List[Schema], sf_db_name, sf_schema_name):
#   conif_file_dir = config.outputdir.pg_sf_mapping_config_path
#   os.makedirs(conif_file_dir, exist_ok=True)  # Create the directory if it doesn't exist
#   clients = config.load_data.client_names.split(',')
  
#   csv_file_name = f"{conif_file_dir}\\pg_to_sf_mapping_new.csv"  
#   headers = "client_name", "source_database", "source_schema", "source_table", "source_etl_view", "target_database", "target_schema", "target_table", "load_method", "load_source_data", "refresh_dynamic_table", "description"
#   data = []
  
#   for client in clients:    
#     for schema in schema_data:
#       for table in schema.tables:
#         pg_view_name = f"{schema.name}_{table.name}_vw"
#         row = [client,schema.database,schema.name,table.name,pg_view_name,sf_db_name,sf_schema_name,table.name,table.load_method,'y','y','']
#         data.append(row)
#   sorted_rows = sorted(data, key=lambda x: (x[0], x[1],x[2],x[3]))
  
#   with open(csv_file_name, mode='w', newline='') as file:
#     writer = csv.writer(file)
#     writer.writerow(headers)
#     writer.writerows(sorted_rows)

#   print("Config file generated successfully")

# def generate_config_element(client_name, database, schema_name,  sf_dynamic_table_db, sf_dynamic_table_schema, table_name, etl_view, load_method):
#   return {
#     "client_name": client_name,
#     "source_database": database,
#     "source_schema": schema_name,
#     "source_table": table_name,
#     "source_etl_view": etl_view,
#     "target": [
#       {
#         "target_database": sf_dynamic_table_db,
#         "target_schema": sf_dynamic_table_schema,
#         "target_table": table_name
#       }
#     ],
#     "load_method": load_method,
#     "load": "y",
#     "description": ""
#   }



# read_config_file()
generate_db_objects_using_config('local', 'greyhavens')
# generate_db_objects_using_schemas('local', 'greyhavens', 'core', 'codes')



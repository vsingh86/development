import configparser
import json
import os
from typing import List
import psycopg2
from psycopg2 import sql
from sshtunnel import SSHTunnelForwarder


class Column:
  def __init__(self, name: str, data_type: str, numeric_precision: int = None, numeric_scale: int = None, sf_data_type: str = ""):
    self.name: str = name
    self.data_type: str = data_type
    self.numeric_precision: int = numeric_precision
    self.numeric_scale: int = numeric_scale
    self.sf_data_type: str = ""
  
    if self.data_type == "uuid" or self.data_type.startswith("character"):
      self.sf_data_type = 'text'
    elif self.data_type.startswith("timestamp"):
      self.sf_data_type = "timestamp"
    elif self.data_type == "numeric":
      self.sf_data_type = f"numeric ({self.numeric_precision}, {self.numeric_scale})"
  
class Table:
  def __init__(self, name: str):
    self.name: str = name
    self.load_method: str = ""
    self.columns: List[Column] = []

  def add_column(self, column: Column) -> None:
    self.columns.append(column)

class Schema:
  def __init__(self, database: str, name: str ):
    self.name: str = name
    self.database: str = database
    self.tables: List[Table] = []

  def add_table(self, table: Table) -> None:
    self.tables.append(table)


def config(section):
  # Create a parser
  parser = configparser.ConfigParser()
  filename='glue\\code-generator\\config.ini',
  # Read config file
  parser.read(filename)

  # Get section, default to postgresql
  db = {}
  if parser.has_section(section):
    params = parser.items(section)
    for param in params:
      db[param[0]] = param[1]
  else:
    raise Exception('Section {0} not found in the {1} file'.format(section, filename))

  return db

def get_db_connection(environment: str):
  dbparams = config(section=f"postgresql_{environment}")
  SSH_HOST = dbparams.get('ssh_host')
  SSH_USERNAME = dbparams.get('ssh_username')
  SSH_PRIVATE_KEY = dbparams.get('ssh_private_key')
  DB_HOST = dbparams.get('db_host')
  DB_PORT = int(dbparams.get('db_port'))
  DB_USER = dbparams.get('db_user')
  DB_PASSWORD = dbparams.get('db_password')
  DB_NAME = dbparams.get('db_name')
  
  if (environment == "dev"):
    # Setting up an SSH tunnel to the bastion server
    with SSHTunnelForwarder(
          (SSH_HOST, 22),  # SSH server endpoint and port
          ssh_username=SSH_USERNAME,
          ssh_pkey=SSH_PRIVATE_KEY,  # Alternatively, use ssh_private_key_password if needed
          remote_bind_address=(DB_HOST, DB_PORT)) as tunnel:
      # Connect to PostgreSQL through the SSH tunnel
      conn = psycopg2.connect(
          dbname=DB_NAME,
          user=DB_USER,
          password=DB_PASSWORD,
          host='localhost',  # Connect to the forwarded port on localhost
          port=tunnel.local_bind_port  # Dynamically assigned local port
      )
  else:
      conn = psycopg2.connect(
          dbname=DB_NAME,
          user=DB_USER,
          password=DB_PASSWORD,
          host=DB_HOST,
          port=DB_PORT
      )
  return conn
  
  
def get_load_method (columns):
  created_at_exists = any(col[0] == 'created_at' for col in columns)
  updated_at_exists = any(col[0] == 'updated_at' for col in columns)
  load_method = "full"
  if created_at_exists: 
    load_method = "incremental"
  if updated_at_exists:
    load_method = "merge"
  return load_method

def get_schema_data(pg_database, schema_name): 
  conn = get_db_connection("local")
  
  cur = conn.cursor()
  # Dictionary to hold the data
  schema = Schema(pg_database, schema_name)

  # Execute the query to get all tables in the schema
  cur.execute(sql.SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = %s"), [schema_name])
  tables = cur.fetchall()

  for (table_name,) in tables:
    table = Table(table_name)
    cur.execute(sql.SQL(
        "SELECT column_name, data_type, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_schema = %s AND table_name = %s"),
        [schema_name, table_name]
    )
    columns = cur.fetchall()
    
    # Loop through columns and add details to the table_info
    for column in columns:
      column_obj = Column(*column)
      table.add_column(column_obj)
    
    table.load_method = get_load_method(columns)
    # Append the table info to the schema_data list
    schema.add_table(table)
  return schema

    
def generate_db_objects(pg_database, pg_schema_name, sf_dynamic_table_db, sf_dynamic_table_schema):
  schema_data = get_schema_data(pg_database, pg_schema_name)
  # generate_pg_objects(schema_data)
  # generate_sf_tables(schema_data)
  # generate_sf_build_script(schema_data)
  # generate_config(schema_data, sf_dynamic_table_db, sf_dynamic_table_schema)
  # generate_sf_dynamic_tables(schema_data, sf_dynamic_table_db, sf_dynamic_table_schema)

def generate_pg_objects(schema_data: Schema):
    # Directory where the SQL files will be saved
  pg_etl_views_dir = config('outputdir').get('pg_etl_views_path')
  pg_migration_script_dir = config('outputdir').get('pg_migration_script_path')
  pg_grant_script_dir = config('outputdir').get('pg_grant_script_path')
  
  os.makedirs(pg_etl_views_dir, exist_ok=True)  
  os.makedirs(pg_migration_script_dir, exist_ok=True)
  os.makedirs(pg_grant_script_dir, exist_ok=True)
  
  pg_migration_script = ""
  pg_grant_script = ""

  for table in schema_data.tables:
    columns_list = ', '.join([f"\n\t\tt.{col.name}" for col in table.columns])
    
    columns_list = ', '.join([
      f"\n\t\tto_jsonb(t.{col.name}) as {col.name}"  if col.data_type.lower() == "array" else
      f"\n\t\tt.{col.name}"
      for col in table.columns
    ])

    join_clause = ""
    if table.load_method == 'incremental': 
      join_clause = f"\n\tLEFT JOIN etl_configuration.data_processing_status s ON ('{schema_data.name}' = s.schema_name and '{table.name}' = s.table_name) \n\tWHERE \n\t\ts.last_processed_timestamp is null or t.created_at > s.last_processed_timestamp"
    
    if table.load_method == 'merge':
      join_clause = f"\n\tLEFT JOIN etl_configuration.data_processing_status s ON ('{schema_data.name}' = s.schema_name and '{table.name}' = s.table_name) \n\tWHERE \n\t\ts.last_processed_timestamp is null or t.updated_at > s.last_processed_timestamp"

    view_name = f"{schema_data.name}_{table.name}_vw"
    view_sql = f"DROP VIEW IF EXISTS etl_configuration.{view_name};\nCREATE OR REPLACE VIEW etl_configuration.{view_name} \nAS \n\tSELECT {columns_list} \n\tFROM {schema_data.name}.{table.name} t{join_clause};"
    view_sql_with_select = f"{view_sql} \n\n-- SELECT * FROM etl_configuration.{view_name};"
    pg_migration_script = f"{pg_migration_script} \n\n{view_sql}"
    
    grant_sql = f"grant select on etl_configuration.{view_name} to etl_role;"
    pg_grant_script = f"{pg_grant_script} \n{grant_sql}"

    file_path = os.path.join(pg_etl_views_dir, f"{view_name}.sql")
    with open(file_path, "w") as file:
      file.write(view_sql_with_select)
    
  #Generate Postgres Migration Script  
  migration_file_path = os.path.join(pg_migration_script_dir, "migration.sql")
  with open(migration_file_path, "w") as migration_file:
    migration_file.write(pg_migration_script.upper())
    
  #Generate Postgres Grant Script  
  grant_file_path = os.path.join(pg_grant_script_dir, "grant.sql")
  with open(grant_file_path, "w") as grant_file:
    grant_file.write(pg_grant_script.upper())
    
def generate_sf_tables(schema_data: Schema):
  # Directory where the SQL files will be saved
  sf_staging_tables_path= config('outputdir').get('sf_staging_tables_path')
  clients = config('clients').get('client_names').split(',')
  for client in clients:        
    for table in schema_data.tables:
      sf_table_output_dir = f"{sf_staging_tables_path}\\data_ingestion\\schemas\\greyhavens_{client}_{schema_data.name}\\tables"  
      os.makedirs(sf_table_output_dir, exist_ok=True)  # Create the directory if it doesn't exist
      columns_list = ', '.join([f"\n\t{col.name} {col.sf_data_type}"for col in table.columns])
      sf_table_name = f"data_ingestion.greyhavens_{client}_{schema_data.name}.{table.name}"
      sf_stage_table_name = f"data_ingestion.greyhavens_{client}_{schema_data.name}.stage_{table.name}"
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
      
def generate_sf_dynamic_tables(schema_data: Schema, sf_db_name, sf_schema_name):
  sf_dynamic_tables_path = config('outputdir').get('sf_dynamic_tables_path')
  clients = config('clients').get('client_names').split(',')
  
  for table in schema_data.tables:
    sf_dynamic_table_output_dir = f"{sf_dynamic_tables_path}\\{sf_db_name}\\{sf_schema_name}\\tables"  
    os.makedirs(sf_dynamic_table_output_dir, exist_ok=True)  # Create the directory if it doesn't exist
    columns_list = ', '.join([
      f"\n\t\tto_binary(replace({col.name},'-', '') as {col.name}"  if col.data_type == "uuid" else
      f"\n\t\t{col.name}"
      for col in table.columns
    ])
    
    dynamic_table_name = f"{sf_db_name}.{sf_schema_name}.{table.name}"
    dynamic_table_sql_drop = f"DROP DYNAMIC TABLE IF EXISTS {dynamic_table_name}"
    dynamic_table_sql_create = f"CREATE OR REPLACE DYNAMIC TABLE {dynamic_table_name} \nTARGET_LAG='DOWNSTREAM' \nWAREHOUSE = ETL_WH \nAS"
    dynamic_table_sql_select = ""
    file_path = os.path.join(sf_dynamic_table_output_dir, f"{table.name}.sql")
    for idx, client in enumerate(clients):
      staging_table_name = f"data_ingestion.greyhavens_{client}_{schema_data.name}.{table.name}"
      if idx > 0:
        dynamic_table_sql_select = f"{dynamic_table_sql_select}\n\tUNION"
      dynamic_table_sql_select = f"{dynamic_table_sql_select}\n\tSELECT {columns_list} \n\tFROM {staging_table_name}"
    
    dynamic_table_sql_select_test = f"\n-- SELECT * FROM {dynamic_table_name} LIMIT 5"
    dynamic_table_sql_final = f"{dynamic_table_sql_drop};\n{dynamic_table_sql_create}{dynamic_table_sql_select};\n{dynamic_table_sql_select_test};"
    with open(file_path, "w") as file:
      file.write(dynamic_table_sql_final.upper())

    
      
def generate_sf_build_script(schema_data: Schema):
    clients = config('clients').get('client_names').split(',')
    sf_build_script_dir = config('outputdir').get('sf_build_script_path')
    os.makedirs(sf_build_script_dir, exist_ok=True)  # Create the directory if it doesn't exist
    build_script_file_path = os.path.join(sf_build_script_dir, "build.sql")
    with open(build_script_file_path, "w") as file:
      for client in clients:        
        build_sql = f"-f data_ingestion\\schemas\\{schema_data.database}_{client}_{schema_data.name}\\{schema_data.database}_{client}_{schema_data.name}.sql `\n"
        file.write(build_sql) 
        for table in schema_data.tables:
          build_sql = f"-f data_ingestion\\schemas\\{schema_data.database}_{client}_{schema_data.name}\\tables\\{table.name}.sql `\n"
          if (table.load_method == "merge"):
            build_sql = f"{build_sql}-f data_ingestion\\schemas\\{schema_data.database}_{client}_{schema_data.name}\\tables\\stage_{table.name}.sql `\n"
          file.write(build_sql) 

def generate_config(schema_data: Schema, sf_db_name, sf_schema_name):
  # Directory where the SQL files will be saved
  conif_file_dir = config('outputdir').get('pg_sf_mapping_config_path')
  os.makedirs(conif_file_dir, exist_ok=True)  # Create the directory if it doesn't exist
  clients = config('clients').get('client_names').split(',')
  # #Generate Configuration File
  config_script = []
  for client in clients:        
    for table in schema_data.tables:
      pg_view_name = f"{schema_data.name}_{table.name}_vw"
      config_script.append(generate_config_element(client,schema_data.database, schema_data.name, sf_db_name, sf_schema_name, table.name, pg_view_name, table.load_method))
  
  config_file_path = os.path.join(conif_file_dir, "sf_to_pg_config.json")
  with open(config_file_path, "w") as file:
    json.dump(config_script, file, indent=2)

def generate_config_element(client_name, database, schema_name,  sf_dynamic_table_db, sf_dynamic_table_schema, table_name, etl_view, load_method):
  return {
    "client_name": client_name,
    "source_database": database,
    "source_schema": schema_name,
    "source_table": table_name,
    "source_etl_view": etl_view,
    "target": [
      {
        "target_database": sf_dynamic_table_db,
        "target_schema": sf_dynamic_table_schema,
        "target_table": table_name
      }
    ],
    "load_method": load_method,
    "load": "y",
    "description": ""
  }

generate_db_objects('greyhavens', 'cg_facility', 'core', 'codes')

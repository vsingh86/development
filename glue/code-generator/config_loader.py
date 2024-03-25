import configparser
from typing import NamedTuple

class PostgresqlConfig(NamedTuple):
    ssh_host: str
    ssh_username: str
    ssh_private_key: str
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str

class LoadDataConfig(NamedTuple):
    client_names: str
    schema_names: str

class OutputDirConfig(NamedTuple):
    pg_etl_views_path: str
    pg_migration_script_path: str
    pg_grant_script_path: str
    sf_staging_tables_path: str
    sf_dynamic_tables_path: str
    sf_build_script_path: str
    pg_sf_mapping_config_path: str

class AppConfig(NamedTuple):
    postgresql: PostgresqlConfig
    load_data: LoadDataConfig
    outputdir: OutputDirConfig

def load_config(config_file_path: str) -> AppConfig:
    parser = configparser.ConfigParser()
    parser.read(config_file_path)
    
    postgresql = PostgresqlConfig(**parser['postgresql'])
    load_data = LoadDataConfig(**parser['load_data'])
    outputdir = OutputDirConfig(**parser['outputdir'])
    
    return AppConfig(postgresql=postgresql, load_data=load_data, outputdir=outputdir)

import os
from sqlalchemy import create_engine, inspect
import boto3
from botocore.exceptions import ClientError
import json
import pandas as pd
from dotenv import load_dotenv
import logging
import sys
import socket
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

def get_aws_secret(secret_name, region_name="eu-west-3"):
    """
    Retrieve database credentials from AWS Secrets Manager
    
    Args:
        secret_name (str): Name of the secret in AWS Secrets Manager
        region_name (str): AWS region where the secret is stored
    
    Returns:
        dict: Database connection credentials
    """
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    
    if 'SecretString' in get_secret_value_response:
        return json.loads(get_secret_value_response['SecretString'])
    
    raise ValueError("No secret found")

def migrate_tables(source_engine, target_engine, tables_to_migrate=None):
    """
    Migrate tables from source database to target database
    
    Args:
        source_engine (sqlalchemy.engine.base.Engine): Source database engine
        target_engine (sqlalchemy.engine.base.Engine): Target database engine
        tables_to_migrate (list, optional): List of specific tables to migrate
    """
    # Inspect source database
    inspector = inspect(source_engine)
    
    if not tables_to_migrate:
        tables_to_migrate = inspector.get_table_names()
    
    for table_name in tables_to_migrate:
        print(f"Migrating table: {table_name}")
        
        # Read entire table into pandas DataFrame
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, source_engine)
        
        # Write DataFrame to target database
        df.to_sql(
            name=table_name, 
            con=target_engine, 
            if_exists='replace',  # Options: 'fail', 'replace', 'append'
            index=False
        )
        print(f"Successfully migrated {len(df)} rows for {table_name}")

def test_database_connection(host, port, username, password, dbname):
    """
    Comprehensive database connection test
    """
    try:
        # Test network connectivity first
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex((host, int(port)))
        sock.close()
        
        if result != 0:
            logging.error(f"Network connection to {host}:{port} failed")
            return False

        # Test SQLAlchemy connection
        connection_string = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
        engine = create_engine(connection_string, connect_args={'connect_timeout': 10})
        
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        
        logging.info("Database connection successful")
        return True
    
    except (socket.error, SQLAlchemyError) as e:
        logging.error(f"Connection error: {e}")
        return False

def create_database(engine, database_name):
    """
    Create a database if it doesn't exist
    
    Args:
        engine (sqlalchemy.engine.base.Engine): Connection engine
        database_name (str): Name of the database to create
    """
    try:
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            
            create_db_stmt = text(f"CREATE DATABASE {database_name}")
            
            try:
                conn.execute(create_db_stmt)
                logging.info(f"Database {database_name} created successfully")
            except sqlalchemy.exc.ProgrammingError as e:
                if "already exists" in str(e):
                    logging.info(f"Database {database_name} already exists")
                else:
                    raise
    except Exception as e:
        logging.error(f"Error creating database {database_name}: {e}")
        raise

def main():
    # Load environment variables
    load_dotenv()

    # Path to the local SQLite database
    sqlite_db_path = os.path.join(os.path.dirname(__file__), 'dbs', 'news_scrapper.db')
    
    # Retrieve RDS credentials from AWS Secrets Manager
    secret_name = "rds!db-ed1bf464-b0d0-4041-8c9c-5604be36e2fe"
    rds_credentials = get_aws_secret(secret_name)

    # RDS Connection Details
    rds_host = "sevenbots.c9w2g0i8kg7w.eu-west-3.rds.amazonaws.com"
    rds_port = "5432"
    
    # Important: Use the default 'postgres' database to first connect and create the target database
    default_dbname = "postgres"
    target_dbname = "sevenbots"  # The database you want to create/use

    # Logging configuration
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # First, connect to the default database and create the target database if it doesn't exist
    try:
        # Connect to default database
        default_engine = create_engine(
            f"postgresql://{rds_credentials['username']}:{rds_credentials['password']}@"
            f"{rds_host}:{rds_port}/{default_dbname}",
            connect_args={'connect_timeout': 20}
        )

        # Create database using the new function
        create_database(default_engine, target_dbname)

        default_engine.dispose()
    except Exception as e:
        logging.error(f"Error creating database: {e}")
        sys.exit(1)

    # Test connection before migration
    if not test_database_connection(
        rds_host, 
        rds_port, 
        rds_credentials['username'], 
        rds_credentials['password'], 
        target_dbname
    ):
        logging.error("Cannot proceed with migration due to connection issues")
        sys.exit(1)

    # Create source and target database engines
    source_engine = create_engine(f'sqlite:///{sqlite_db_path}')
    target_engine = create_engine(
        f"postgresql://{rds_credentials['username']}:{rds_credentials['password']}@"
        f"{rds_host}:{rds_port}/{target_dbname}",
        connect_args={'connect_timeout': 20}  # Increased timeout
    )
    
    try:
        migrate_tables(source_engine, target_engine)
        logging.info("Migration completed successfully!")
    except Exception as e:
        logging.error(f"Migration failed: {e}")
    finally:
        source_engine.dispose()
        target_engine.dispose()

if __name__ == "__main__":
    main()

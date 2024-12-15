import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
import boto3
from botocore.exceptions import ClientError
import logging
import sys

def get_aws_secret(secret_name, region_name="eu-west-3"):
    """
    Retrieve database credentials from AWS Secrets Manager
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

def read_prompts_from_folder(base_path):
    """
    Read prompts from the Prompts folder and create a DataFrame
    """
    prompts_data = []
    
    # Iterate through levels
    for level_folder in os.listdir(base_path):
        level_path = os.path.join(base_path, level_folder)
        
        # Check if it's a directory and starts with 'level'
        if os.path.isdir(level_path) and level_folder.startswith('level'):
            # Iterate through agents in the level
            for agent_folder in os.listdir(level_path):
                agent_path = os.path.join(level_path, agent_folder)
                
                if os.path.isdir(agent_path):
                    # Initialize variables
                    config = {}
                    assistant_prompt = ""
                    decision_prompt = ""
                    system_prompt = ""
                    
                    # Read config file
                    config_path = os.path.join(agent_path, 'config.json')
                    if os.path.exists(config_path):
                        with open(config_path, 'r') as f:
                            config = json.load(f)
                    
                    # Read assistant prompt
                    assistant_prompt_path = os.path.join(agent_path, 'assistant_prompt.j2')
                    if os.path.exists(assistant_prompt_path):
                        with open(assistant_prompt_path, 'r') as f:
                            assistant_prompt = f.read()
                    
                    # Read decision prompt
                    decision_prompt_path = os.path.join(agent_path, 'decision_prompt.j2')
                    if os.path.exists(decision_prompt_path):
                        with open(decision_prompt_path, 'r') as f:
                            decision_prompt = f.read()
                    
                    # Read system prompt
                    system_prompt_path = os.path.join(agent_path, 'system_prompt.j2')
                    if os.path.exists(system_prompt_path):
                        with open(system_prompt_path, 'r') as f:
                            system_prompt = f.read()
                    
                    # Append data
                    prompts_data.append({
                        'user': 'karim',
                        'agent_name': agent_folder,
                        'agent_level': level_folder,
                        'assistant_prompt': assistant_prompt,
                        'decision_prompt': decision_prompt,
                        'system_prompt': system_prompt,
                        'config': json.dumps(config)
                    })
    
    return pd.DataFrame(prompts_data)

def create_prompts_table(engine, df):
    """
    Create a table in PostgreSQL and insert the DataFrame
    """
    # Create table SQL
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS prompts (
        id SERIAL PRIMARY KEY,
        user_str VARCHAR(255),
        agent_name VARCHAR(255),
        agent_level VARCHAR(50),
        assistant_prompt TEXT,
        decision_prompt TEXT,
        system_prompt TEXT,
        config JSONB
    )
    """
    
    try:
        with engine.connect() as conn:
            # Create table
            conn.execute(text(create_table_sql))
            
            # Insert data
            for _, row in df.iterrows():
                insert_sql = text("""
                INSERT INTO prompts (
                    user_str, agent_name, agent_level, 
                    assistant_prompt, decision_prompt, system_prompt, config
                ) VALUES (
                    :user, :agent_name, :agent_level, 
                    :assistant_prompt, :decision_prompt, :system_prompt, :config
                )
                """)
                
                conn.execute(insert_sql, {
                    'user': row['user'],
                    'agent_name': row['agent_name'],
                    'agent_level': row['agent_level'],
                    'assistant_prompt': row['assistant_prompt'],
                    'decision_prompt': row['decision_prompt'],
                    'system_prompt': row['system_prompt'],
                    'config': row['config']
                })
            
            conn.commit()
            logging.info("Successfully created and populated prompts table")
    
    except Exception as e:
        logging.error(f"Error creating prompts table: {e}")
        raise

def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # Path to Prompts folder
    prompts_path = os.path.join(os.path.dirname(__file__), 'Prompts')

    # Read prompts into DataFrame
    prompts_df = read_prompts_from_folder(prompts_path)
    logging.info(f"Found {len(prompts_df)} agents")

    # Retrieve RDS credentials from AWS Secrets Manager
    secret_name = "rds!db-ed1bf464-b0d0-4041-8c9c-5604be36e2fe"
    rds_credentials = get_aws_secret(secret_name)

    # RDS Connection Details
    rds_host = "sevenbots.c9w2g0i8kg7w.eu-west-3.rds.amazonaws.com"
    rds_port = "5432"
    target_dbname = "sevenbots"

    # Create database engine
    target_engine = create_engine(
        f"postgresql://{rds_credentials['username']}:{rds_credentials['password']}@"
        f"{rds_host}:{rds_port}/{target_dbname}",
        connect_args={'connect_timeout': 20}
    )

    # Create and populate prompts table
    create_prompts_table(target_engine, prompts_df)

if __name__ == "__main__":
    main()

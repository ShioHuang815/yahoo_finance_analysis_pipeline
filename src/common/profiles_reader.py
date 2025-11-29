"""
Profiles Reader - Single Source of Truth for Snowflake Credentials
Reads dbt profiles.yml and extracts Snowflake connection parameters.
"""
import os
import yaml
from pathlib import Path
from typing import Dict, Any


def read_profiles(
    profiles_path: str = "/usr/local/airflow/include/project_name/profiles.yml",
    profile_name: str = "finance_analysis_pipeline",
    target: str = "dev"
) -> Dict[str, Any]:
    """
    Read dbt profiles.yml and extract Snowflake connection parameters.
    
    Args:
        profiles_path: Path to profiles.yml file
        profile_name: Name of the dbt profile to use
        target: Target environment (default: dev)
    
    Returns:
        Dictionary with Snowflake connection parameters
    """
    if not os.path.exists(profiles_path):
        raise FileNotFoundError(f"Profiles file not found: {profiles_path}")
    
    with open(profiles_path, 'r') as f:
        profiles = yaml.safe_load(f)
    
    if profile_name not in profiles:
        raise ValueError(f"Profile '{profile_name}' not found in {profiles_path}")
    
    profile = profiles[profile_name]
    
    if 'outputs' not in profile or target not in profile['outputs']:
        raise ValueError(f"Target '{target}' not found in profile '{profile_name}'")
    
    output_config = profile['outputs'][target]
    
    return {
        'account': output_config.get('account'),
        'user': output_config.get('user'),
        'role': output_config.get('role'),
        'warehouse': output_config.get('warehouse'),
        'database': output_config.get('database'),
        'schema': output_config.get('schema'),
        'private_key_path': output_config.get('private_key_path'),
        'type': output_config.get('type'),
        'threads': output_config.get('threads', 1)
    }


def get_snowflake_connection_params(
    profiles_path: str = "/usr/local/airflow/include/project_name/profiles.yml",
    profile_name: str = "finance_analysis_pipeline"
) -> Dict[str, Any]:
    """
    Get Snowflake connection parameters ready for snowflake.connector.
    
    Returns:
        Dictionary with parameters for snowflake.connector.connect()
    """
    config = read_profiles(profiles_path, profile_name)
    
    # Read private key if path is provided
    private_key_bytes = None
    if config.get('private_key_path'):
        private_key_path = config['private_key_path']
        
        # Try container path first, then local path
        local_path = private_key_path.replace('/usr/local/airflow/', '')
        
        if os.path.exists(private_key_path):
            with open(private_key_path, 'rb') as key_file:
                private_key_bytes = key_file.read()
        elif os.path.exists(local_path):
            # Use local path for testing
            with open(local_path, 'rb') as key_file:
                private_key_bytes = key_file.read()
        else:
            raise FileNotFoundError(f"Private key not found at {private_key_path} or {local_path}")
    
    return {
        'account': config['account'],
        'user': config['user'],
        'role': config['role'],
        'warehouse': config['warehouse'],
        'database': config['database'],
        'schema': config['schema'],
        'private_key': private_key_bytes
    }

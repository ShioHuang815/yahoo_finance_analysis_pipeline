"""
Snowflake Loader - Load DataFrames to Snowflake RAW schema
Manages metadata and watermarks for ingestion tracking.
"""
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from src.common.logging import setup_logger
from src.common.profiles_reader import get_snowflake_connection_params
from src.common.state_store import generate_run_id

logger = setup_logger(__name__)


class SnowflakeLoader:
    """Handles loading data to Snowflake and managing metadata."""
    
    def __init__(self, profiles_path: str = "/usr/local/airflow/include/project_name/profiles.yml"):
        """
        Initialize Snowflake loader.
        
        Args:
            profiles_path: Path to dbt profiles.yml file
        """
        self.profiles_path = profiles_path
        self.conn = None
        
    def _get_connection(self) -> snowflake.connector.SnowflakeConnection:
        """Create and return Snowflake connection."""
        if self.conn and not self.conn.is_closed():
            return self.conn
        
        logger.info("Establishing Snowflake connection")
        params = get_snowflake_connection_params(self.profiles_path)
        
        # Parse private key
        private_key_obj = None
        if params.get('private_key'):
            private_key_obj = serialization.load_pem_private_key(
                params['private_key'],
                password=None,  # No passphrase
                backend=default_backend()
            )
            pkb = private_key_obj.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
        
        self.conn = snowflake.connector.connect(
            account=params['account'],
            user=params['user'],
            role=params['role'],
            warehouse=params['warehouse'],
            database=params['database'],
            schema=params['schema'],
            private_key=pkb if private_key_obj else None
        )
        
        logger.info(f"Connected to Snowflake: {params['database']}.{params['schema']}")
        return self.conn
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute a query and return results as DataFrame.
        
        Args:
            query: SQL query to execute
            params: Query parameters
        
        Returns:
            DataFrame with query results
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(query, params)
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                return pd.DataFrame(data, columns=columns)
            return pd.DataFrame()
        finally:
            cursor.close()
    
    def load_to_raw(
        self,
        df: pd.DataFrame,
        table_name: str,
        dataset_name: str,
        create_table: bool = False
    ) -> Dict[str, Any]:
        """
        Load DataFrame to RAW schema table.
        
        Args:
            df: DataFrame to load
            table_name: Target table name in RAW schema
            dataset_name: Dataset identifier for metadata tracking
            create_table: Whether to create table if it doesn't exist
        
        Returns:
            Dictionary with load statistics
        """
        if df.empty:
            logger.warning(f"Empty DataFrame provided for {table_name}")
            return {'records_loaded': 0, 'status': 'skipped'}
        
        conn = self._get_connection()
        run_id = generate_run_id()
        
        # Make a copy to avoid modifying the original
        df = df.copy()
        
        # Convert date columns to proper date format (not timestamp)
        for col in df.columns:
            if col.lower() == 'date' or 'date' in col.lower():
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.date
        
        # Add run_id to DataFrame
        df['source_run_id'] = run_id
        
        logger.info(f"Loading {len(df)} records to {table_name}")
        
        try:
            # Use write_pandas for efficient bulk loading
            success, num_chunks, num_rows, output = write_pandas(
                conn=conn,
                df=df,
                table_name=table_name,
                database=conn.database,
                schema='RAW',
                auto_create_table=create_table,
                overwrite=False,  # Append mode
                quote_identifiers=False
            )
            
            if success:
                logger.info(f"Successfully loaded {num_rows} rows to RAW.{table_name}")
                
                # Log to metadata
                self._log_ingest_run(
                    run_id=run_id,
                    dataset_name=dataset_name,
                    table_name=table_name,
                    records_loaded=num_rows,
                    status='completed'
                )
                
                return {
                    'run_id': run_id,
                    'records_loaded': num_rows,
                    'status': 'completed',
                    'chunks': num_chunks
                }
            else:
                raise Exception(f"Load failed: {output}")
        
        except Exception as e:
            logger.error(f"Error loading to {table_name}: {str(e)}")
            self._log_ingest_run(
                run_id=run_id,
                dataset_name=dataset_name,
                table_name=table_name,
                records_loaded=0,
                status='failed',
                error_message=str(e)
            )
            raise
    
    def _log_ingest_run(
        self,
        run_id: str,
        dataset_name: str,
        table_name: str,
        records_loaded: int,
        status: str,
        error_message: Optional[str] = None
    ):
        """Log ingestion run to METADATA.INGEST_RUNS table."""
        try:
            query = """
            INSERT INTO METADATA.INGEST_RUNS 
            (run_id, dataset_name, table_name, run_timestamp, records_loaded, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor = self.conn.cursor()
            cursor.execute(query, (
                run_id,
                dataset_name,
                table_name,
                datetime.now(),
                records_loaded,
                status,
                error_message
            ))
            cursor.close()
            
            logger.debug(f"Logged run {run_id} to metadata")
        except Exception as e:
            logger.warning(f"Failed to log metadata: {str(e)}")
    
    def update_watermark(self, dataset_name: str, watermark_value: Any):
        """Update watermark for a dataset."""
        try:
            query = """
            MERGE INTO METADATA.WATERMARKS w
            USING (SELECT %s AS dataset_name, %s AS watermark_value, %s AS updated_at) s
            ON w.dataset_name = s.dataset_name
            WHEN MATCHED THEN
                UPDATE SET w.watermark_value = s.watermark_value, w.updated_at = s.updated_at
            WHEN NOT MATCHED THEN
                INSERT (dataset_name, watermark_value, updated_at)
                VALUES (s.dataset_name, s.watermark_value, s.updated_at)
            """
            
            cursor = self.conn.cursor()
            cursor.execute(query, (dataset_name, str(watermark_value), datetime.now()))
            cursor.close()
            
            logger.info(f"Updated watermark for {dataset_name}: {watermark_value}")
        except Exception as e:
            logger.warning(f"Failed to update watermark: {str(e)}")
    
    def get_watermark(self, dataset_name: str) -> Optional[str]:
        """Get current watermark for a dataset."""
        try:
            query = """
            SELECT watermark_value 
            FROM METADATA.WATERMARKS 
            WHERE dataset_name = %s
            """
            
            cursor = self.conn.cursor()
            cursor.execute(query, (dataset_name,))
            result = cursor.fetchone()
            cursor.close()
            
            return result[0] if result else None
        except Exception as e:
            logger.warning(f"Failed to get watermark: {str(e)}")
            return None
    
    def close(self):
        """Close Snowflake connection."""
        if self.conn and not self.conn.is_closed():
            self.conn.close()
            logger.info("Snowflake connection closed")


if __name__ == "__main__":
    # Test connection
    loader = SnowflakeLoader()
    
    try:
        # Test query
        result = loader.execute_query("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_ROLE()")
        print("\nSnowflake Connection Test:")
        print(result)
    finally:
        loader.close()

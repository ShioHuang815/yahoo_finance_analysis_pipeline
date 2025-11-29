"""
Finance Analysis Pipeline - Airflow DAG
Orchestrates extraction from Yahoo Finance, loading to Snowflake, and dbt transformations.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import yaml
import sys
import os

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.extract.yahoo_prices import extract_yahoo_prices
from src.extract.yahoo_company_info import extract_yahoo_company_info
from src.extract.yahoo_benchmark_series import extract_yahoo_benchmark_series
from src.load.snowflake_loader import SnowflakeLoader
from src.common.logging import setup_logger

logger = setup_logger(__name__)

# Default arguments
default_args = {
    'owner': 'finance_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Load ticker configuration
TICKER_CONFIG_PATH = '/usr/local/airflow/config/tickers.yaml'


def load_ticker_config():
    """Load ticker configuration from YAML file."""
    with open(TICKER_CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)


def extract_and_load_prices(**context):
    """Extract stock prices and load to Snowflake."""
    logger.info("Starting price extraction")
    
    # Load config
    config = load_ticker_config()
    tickers = config['stocks']
    
    # Extract data (last 30 days)
    df = extract_yahoo_prices(tickers, period="1mo")
    
    logger.info(f"Extracted {len(df)} price records")
    
    # Load to Snowflake
    loader = SnowflakeLoader(profiles_path='/usr/local/airflow/include/finance_analysis_pipeline/profiles.yml')
    try:
        result = loader.load_to_raw(
            df=df,
            table_name='STOCK_PRICES_DAILY',
            dataset_name='stock_prices',
            create_table=False  # Table should already exist
        )
        
        # Update watermark with latest date
        if not df.empty:
            max_date = df['date'].max()
            loader.update_watermark('stock_prices', str(max_date))
        
        logger.info(f"Load result: {result}")
        
        # Push to XCom for downstream tasks
        context['task_instance'].xcom_push(key='records_loaded', value=result['records_loaded'])
        
    finally:
        loader.close()


def extract_and_load_company_info(**context):
    """Extract company info and load to Snowflake."""
    logger.info("Starting company info extraction")
    
    # Load config
    config = load_ticker_config()
    tickers = config['stocks']
    
    # Extract data
    df = extract_yahoo_company_info(tickers)
    
    logger.info(f"Extracted {len(df)} company records")
    
    # Load to Snowflake
    loader = SnowflakeLoader(profiles_path='/usr/local/airflow/include/finance_analysis_pipeline/profiles.yml')
    try:
        result = loader.load_to_raw(
            df=df,
            table_name='COMPANY_INFO',
            dataset_name='company_info',
            create_table=False
        )
        
        logger.info(f"Load result: {result}")
        context['task_instance'].xcom_push(key='records_loaded', value=result['records_loaded'])
        
    finally:
        loader.close()


def extract_and_load_benchmarks(**context):
    """Extract benchmark series and load to Snowflake."""
    logger.info("Starting benchmark extraction")
    
    # Load config
    config = load_ticker_config()
    benchmarks = config['benchmarks']
    
    # Extract data (last 30 days)
    df = extract_yahoo_benchmark_series(benchmarks, period="1mo")
    
    logger.info(f"Extracted {len(df)} benchmark records")
    
    # Load to Snowflake
    loader = SnowflakeLoader(profiles_path='/usr/local/airflow/include/finance_analysis_pipeline/profiles.yml')
    try:
        result = loader.load_to_raw(
            df=df,
            table_name='BENCHMARK_SERIES_DAILY',
            dataset_name='benchmark_series',
            create_table=False
        )
        
        # Update watermark
        if not df.empty:
            max_date = df['date'].max()
            loader.update_watermark('benchmark_series', str(max_date))
        
        logger.info(f"Load result: {result}")
        context['task_instance'].xcom_push(key='records_loaded', value=result['records_loaded'])
        
    finally:
        loader.close()


def verify_data_quality(**context):
    """Verify data was loaded successfully."""
    logger.info("Starting data quality verification")
    
    loader = SnowflakeLoader(profiles_path='/usr/local/airflow/include/finance_analysis_pipeline/profiles.yml')
    try:
        # Check row counts
        checks = [
            ("COBRA.STOCK_PRICES_DAILY", "stock prices"),
            ("COBRA.COMPANY_INFO", "company info"),
            ("COBRA.BENCHMARK_SERIES_DAILY", "benchmark series")
        ]
        
        results = {}
        for table, name in checks:
            query = f"SELECT COUNT(*) as count FROM {table}"
            result = loader.execute_query(query)
            count = result['COUNT'].iloc[0] if not result.empty else 0
            results[name] = count
            logger.info(f"{name}: {count} total records")
            
            # Ensure we have data
            if count == 0:
                raise ValueError(f"No data found in {table}")
        
        # Check for recent data (within last 7 days)
        recent_check = """
        SELECT MAX(date) as max_date 
        FROM COBRA.STOCK_PRICES_DAILY
        """
        result = loader.execute_query(recent_check)
        if not result.empty:
            max_date = result['MAX_DATE'].iloc[0]
            logger.info(f"Most recent price data: {max_date}")
            
            # Verify freshness (should be within last 7 days)
            from datetime import datetime, timedelta
            if isinstance(max_date, str):
                max_date = datetime.strptime(max_date, '%Y-%m-%d').date()
            
            days_old = (datetime.now().date() - max_date).days
            if days_old > 7:
                logger.warning(f"Data is {days_old} days old - may need refresh")
        
        logger.info("Data quality verification passed")
        return results
        
    finally:
        loader.close()


# Define DAG
with DAG(
    dag_id='finance_pipeline_dag',
    default_args=default_args,
    description='Extract Yahoo Finance data and load to Snowflake',
    schedule='0 2 * * 1-5',  # 2 AM on weekdays
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['finance', 'yahoo', 'snowflake', 'etl'],
) as dag:
    
    # Start marker
    start = EmptyOperator(task_id='start')
    
    # Extract and load tasks (can run in parallel)
    extract_prices = PythonOperator(
        task_id='extract_yahoo_prices',
        python_callable=extract_and_load_prices,
    )
    
    extract_company = PythonOperator(
        task_id='extract_yahoo_company_info',
        python_callable=extract_and_load_company_info,
    )
    
    extract_benchmarks = PythonOperator(
        task_id='extract_yahoo_benchmark_series',
        python_callable=extract_and_load_benchmarks,
    )
    
    # Verification task
    verify = PythonOperator(
        task_id='verify_data_quality',
        python_callable=verify_data_quality,
    )
    
    # dbt transformation task
    transform_dbt = BashOperator(
        task_id='transform_dbt',
        bash_command="""
        cd /usr/local/airflow/dbt_finance && \
        dbt run --profiles-dir /usr/local/airflow/include/finance_analysis_pipeline && \
        dbt test --profiles-dir /usr/local/airflow/include/finance_analysis_pipeline
        """,
    )
    
    # End marker
    end = EmptyOperator(task_id='end')
    
    # Define task dependencies
    # Start -> all extracts in parallel -> verify -> dbt transform -> end
    start >> [extract_prices, extract_company, extract_benchmarks] >> verify >> transform_dbt >> end

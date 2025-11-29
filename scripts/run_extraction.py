"""
Standalone script to extract data from Yahoo Finance and load to Snowflake.
Can be run outside of Airflow for testing or manual execution.
"""
import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.extract.yahoo_prices import extract_yahoo_prices
from src.extract.yahoo_company_info import extract_yahoo_company_info
from src.extract.yahoo_benchmark_series import extract_yahoo_benchmark_series
from src.load.snowflake_loader import SnowflakeLoader
from src.common.logging import setup_logger
import yaml

logger = setup_logger(__name__)


def load_ticker_config():
    """Load ticker configuration from YAML file."""
    config_path = project_root / "config" / "tickers.yaml"
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def extract_and_load_prices():
    """Extract stock prices and load to Snowflake."""
    logger.info("=" * 60)
    logger.info("EXTRACTING STOCK PRICES")
    logger.info("=" * 60)
    
    # Load config
    config = load_ticker_config()
    tickers = config['stocks']
    logger.info(f"Tickers: {', '.join(tickers)}")
    
    # Extract data (last 30 days for initial load)
    logger.info("Fetching data from Yahoo Finance (last 30 days)...")
    df = extract_yahoo_prices(tickers, period="1mo")
    
    if df.empty:
        logger.error("No price data extracted!")
        return
    
    logger.info(f"✓ Extracted {len(df)} price records")
    logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
    logger.info(f"Sample data:\n{df.head()}")
    
    # Load to Snowflake
    profiles_path = str(project_root / "include" / "finance_analysis_pipeline" / "profiles.yml")
    loader = SnowflakeLoader(profiles_path=profiles_path)
    
    try:
        result = loader.load_to_raw(
            df=df,
            table_name='STOCK_PRICES_DAILY',
            dataset_name='stock_prices',
            create_table=False
        )
        
        logger.info(f"✓ Load successful: {result}")
        
        # Update watermark
        if not df.empty:
            max_date = df['date'].max()
            loader.update_watermark('stock_prices', str(max_date))
            logger.info(f"✓ Updated watermark to {max_date}")
        
    finally:
        loader.close()


def extract_and_load_company_info():
    """Extract company info and load to Snowflake."""
    logger.info("=" * 60)
    logger.info("EXTRACTING COMPANY INFO")
    logger.info("=" * 60)
    
    # Load config
    config = load_ticker_config()
    tickers = config['stocks']
    logger.info(f"Tickers: {', '.join(tickers)}")
    
    # Extract data
    logger.info("Fetching company info from Yahoo Finance...")
    df = extract_yahoo_company_info(tickers)
    
    if df.empty:
        logger.error("No company data extracted!")
        return
    
    logger.info(f"✓ Extracted {len(df)} company records")
    logger.info(f"Sample data:\n{df[['symbol', 'company_name', 'sector', 'market_cap']].head()}")
    
    # Load to Snowflake
    profiles_path = str(project_root / "include" / "finance_analysis_pipeline" / "profiles.yml")
    loader = SnowflakeLoader(profiles_path=profiles_path)
    
    try:
        result = loader.load_to_raw(
            df=df,
            table_name='COMPANY_INFO',
            dataset_name='company_info',
            create_table=False
        )
        
        logger.info(f"✓ Load successful: {result}")
        
    finally:
        loader.close()


def extract_and_load_benchmarks():
    """Extract benchmark series and load to Snowflake."""
    logger.info("=" * 60)
    logger.info("EXTRACTING BENCHMARK SERIES")
    logger.info("=" * 60)
    
    # Load config
    config = load_ticker_config()
    benchmarks = config['benchmarks']
    logger.info(f"Benchmarks: {', '.join(benchmarks)}")
    
    # Extract data (last 30 days)
    logger.info("Fetching benchmark data from Yahoo Finance...")
    df = extract_yahoo_benchmark_series(benchmarks, period="1mo")
    
    if df.empty:
        logger.error("No benchmark data extracted!")
        return
    
    logger.info(f"✓ Extracted {len(df)} benchmark records")
    logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
    logger.info(f"Series: {df['series_ticker'].unique().tolist()}")
    
    # Load to Snowflake
    profiles_path = str(project_root / "include" / "finance_analysis_pipeline" / "profiles.yml")
    loader = SnowflakeLoader(profiles_path=profiles_path)
    
    try:
        result = loader.load_to_raw(
            df=df,
            table_name='BENCHMARK_SERIES_DAILY',
            dataset_name='benchmark_series',
            create_table=False
        )
        
        logger.info(f"✓ Load successful: {result}")
        
        # Update watermark
        if not df.empty:
            max_date = df['date'].max()
            loader.update_watermark('benchmark_series', str(max_date))
            logger.info(f"✓ Updated watermark to {max_date}")
        
    finally:
        loader.close()


def main():
    """Run all extraction jobs."""
    print("\n" + "=" * 60)
    print("FINANCE DATA EXTRACTION - MANUAL RUN")
    print("=" * 60 + "\n")
    
    try:
        # Run all extractions
        extract_and_load_prices()
        print("\n")
        
        extract_and_load_company_info()
        print("\n")
        
        extract_and_load_benchmarks()
        print("\n")
        
        print("=" * 60)
        print("✓ ALL EXTRACTIONS COMPLETED SUCCESSFULLY")
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}", exc_info=True)
        print("\n" + "=" * 60)
        print("✗ EXTRACTION FAILED - See logs above")
        print("=" * 60)
        raise


if __name__ == "__main__":
    main()

"""
Yahoo Finance - Benchmark and Macro Series Extractor
Downloads benchmark indices and macro indicators.
"""
import pandas as pd
import yfinance as yf
from datetime import datetime
from typing import List, Optional
from src.common.logging import setup_logger

logger = setup_logger(__name__)


def extract_yahoo_benchmark_series(
    tickers: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    period: str = "1y"
) -> pd.DataFrame:
    """
    Extract benchmark and macro series data from Yahoo Finance.
    
    Args:
        tickers: List of benchmark ticker symbols (e.g., ^GSPC, ^VIX, ^TNX, XLK, XLF)
        start_date: Start date (YYYY-MM-DD format). If None, uses period.
        end_date: End date (YYYY-MM-DD format). If None, uses today.
        period: Period to fetch if start_date is None
    
    Returns:
        DataFrame with columns: series_ticker, date, value (close price), ingested_at
    """
    logger.info(f"Extracting benchmark data for {len(tickers)} series")
    
    try:
        # Download data
        if start_date and end_date:
            logger.info(f"Downloading data from {start_date} to {end_date}")
            data = yf.download(
                tickers,
                start=start_date,
                end=end_date,
                group_by='ticker',
                auto_adjust=False,
                progress=False,
                threads=True
            )
        else:
            logger.info(f"Downloading data for period: {period}")
            data = yf.download(
                tickers,
                period=period,
                group_by='ticker',
                auto_adjust=False,
                progress=False,
                threads=True
            )
        
        # Handle single ticker vs multiple tickers
        if len(tickers) == 1:
            # Single ticker - data structure is simpler
            df = data.copy()
            
            # Flatten MultiIndex columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            
            # Reset index to get date as a column
            df = df.reset_index()
            
            # Find the close price column
            close_col = None
            date_col = None
            
            for col in df.columns:
                col_str = str(col).lower()
                if col_str in ['close', 'adj close', 'adj_close']:
                    close_col = col
                elif col_str in ['date', 'datetime', 'index']:
                    date_col = col
            
            # Select only the columns we need
            if close_col and date_col:
                df = df[[date_col, close_col]].copy()
                df.columns = ['date', 'value']
            else:
                # Fallback: assume first column is date, second is close
                df = df.iloc[:, :2].copy()
                df.columns = ['date', 'value']
            
            df['series_ticker'] = tickers[0]
        else:
            dfs = []
            for ticker in tickers:
                try:
                    ticker_data = data[ticker][['Close']].copy()
                    ticker_data['series_ticker'] = ticker
                    ticker_data = ticker_data.reset_index()
                    ticker_data.columns = ['date', 'value', 'series_ticker']
                    dfs.append(ticker_data)
                except (KeyError, AttributeError) as e:
                    logger.warning(f"No data found for ticker: {ticker}")
                    continue
            
            if not dfs:
                logger.error("No data extracted for any benchmark ticker")
                return pd.DataFrame()
            
            df = pd.concat(dfs, ignore_index=True)
        
        # Reorder columns
        df = df[['series_ticker', 'date', 'value']]
        
        # Add metadata
        df['ingested_at'] = datetime.now()
        
        # Remove rows with missing values
        df = df.dropna(subset=['value'])
        
        logger.info(f"Successfully extracted {len(df)} benchmark records")
        
        return df
    
    except Exception as e:
        logger.error(f"Error extracting benchmark data: {str(e)}")
        raise


if __name__ == "__main__":
    # Test extraction
    import yaml
    
    with open('config/tickers.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    benchmarks = config['benchmarks']
    
    df = extract_yahoo_benchmark_series(benchmarks, period="1mo")
    print(f"\nExtracted {len(df)} benchmark records")
    print(df.head(10))
    print(f"\nUnique series: {df['series_ticker'].unique()}")
    print(f"\nColumns: {df.columns.tolist()}")

"""
Finance Analysis Dashboard
Main entry point for the Streamlit app
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.common.profiles_reader import get_snowflake_connection_params
import snowflake.connector

# Page config
st.set_page_config(
    page_title="Finance Analysis Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize connection
@st.cache_resource
def get_snowflake_connection():
    """Create Snowflake connection using profiles.yml"""
    try:
        # Try streamlit secrets first
        if hasattr(st, 'secrets') and 'snowflake' in st.secrets:
            return snowflake.connector.connect(
                **st.secrets["snowflake"]
            )
        else:
            # Fall back to profiles.yml
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization
            
            params = get_snowflake_connection_params(
                profiles_path='include/finance_analysis_pipeline/profiles.yml'
            )
            
            # Parse private key
            private_key_obj = serialization.load_pem_private_key(
                params['private_key'],
                password=None,
                backend=default_backend()
            )
            pkb = private_key_obj.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            
            return snowflake.connector.connect(
                account=params['account'],
                user=params['user'],
                role=params['role'],
                warehouse=params['warehouse'],
                database=params['database'],
                schema='COBRA_analytics',
                private_key=pkb
            )
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {str(e)}")
        return None

@st.cache_data(ttl=600)
def run_query(query):
    """Run a query and return results as DataFrame"""
    conn = get_snowflake_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            cursor.close()
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            st.error(f"Query error: {str(e)}")
            return pd.DataFrame()
    return pd.DataFrame()

# Sidebar
st.sidebar.title("📊 Finance Dashboard")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigate",
    ["🏠 Home", "🔍 Stock Screener", "📈 Benchmark Analysis"]
)

if page == "🏠 Home":
    st.title("📈 Finance Analysis Dashboard")
    st.markdown("### Welcome to the Finance Analysis Pipeline Dashboard")
    
    col1, col2, col3 = st.columns(3)
    
    # Summary statistics
    with col1:
        st.metric(
            "Total Stocks",
            run_query("SELECT COUNT(DISTINCT symbol) FROM COBRA_analytics.dim_stocks")['COUNT(DISTINCT SYMBOL)'].iloc[0]
            if not run_query("SELECT COUNT(DISTINCT symbol) FROM COBRA_analytics.dim_stocks").empty else 0
        )
    
    with col2:
        max_date_result = run_query("SELECT MAX(date) FROM COBRA_analytics.fact_daily_metrics")
        latest_date = str(max_date_result['MAX(DATE)'].iloc[0]) if not max_date_result.empty else "N/A"
        st.metric("Latest Data", latest_date)
    
    with col3:
        st.metric(
            "Total Records",
            f"{run_query('SELECT COUNT(*) FROM COBRA_analytics.fact_daily_metrics')['COUNT(*)'].iloc[0]:,}"
            if not run_query("SELECT COUNT(*) FROM COBRA_analytics.fact_daily_metrics").empty else 0
        )
    
    st.markdown("---")
    st.markdown("#### 🎯 Features")
    st.markdown("""
    - **Stock Screener**: Filter stocks by sector, market cap, and performance metrics
    - **Benchmark Analysis**: Compare stock performance against market benchmarks
    - **Interactive Charts**: Visualize price trends, returns, and volatility
    """)
    
    st.markdown("#### 📊 Data Pipeline")
    st.markdown("""
    1. **Extract**: Yahoo Finance data (prices, fundamentals, benchmarks)
    2. **Load**: Raw data into Snowflake
    3. **Transform**: dbt models create analytics-ready datasets
    4. **Visualize**: Streamlit dashboard for analysis
    """)

elif page == "🔍 Stock Screener":
    st.title("🔍 Stock Screener")
    
    # Load stock metadata
    stocks_df = run_query("SELECT * FROM COBRA_analytics.dim_stocks")
    
    if not stocks_df.empty:
        # Filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            sectors = ['All'] + sorted(stocks_df['SECTOR'].dropna().unique().tolist())
            selected_sector = st.selectbox("Sector", sectors)
        
        with col2:
            market_caps = ['All', 'Mega Cap', 'Large Cap', 'Mid Cap', 'Small Cap']
            selected_cap = st.selectbox("Market Cap", market_caps)
        
        with col3:
            selected_stock = st.selectbox(
                "Stock",
                ['All'] + sorted(stocks_df['SYMBOL'].unique().tolist())
            )
        
        # Build query
        where_clauses = []
        if selected_sector != 'All':
            where_clauses.append(f"sector = '{selected_sector}'")
        if selected_cap != 'All':
            where_clauses.append(f"market_cap_category = '{selected_cap}'")
        if selected_stock != 'All':
            where_clauses.append(f"f.symbol = '{selected_stock}'")
        
        where_sql = " AND " + " AND ".join(where_clauses) if where_clauses else ""
        
        query = f"""
        SELECT 
            f.symbol,
            f.date,
            f.close,
            f.daily_return,
            CAST(f.volume AS VARCHAR) as volume,
            f.alpha_vs_sp500,
            f.market_cap_category,
            s.company_name,
            s.sector,
            s.industry
        FROM COBRA_analytics.fact_daily_metrics f
        LEFT JOIN COBRA_analytics.dim_stocks s ON f.symbol = s.symbol
        WHERE 1=1 {where_sql}
        ORDER BY f.date DESC
        LIMIT 10000
        """
        
        data = run_query(query)
        
        if not data.empty:
            st.markdown(f"### 📊 Showing {len(data['SYMBOL'].unique())} stocks")
            
            # Price chart
            st.markdown("#### Price Trends")
            fig = px.line(
                data,
                x='DATE',
                y='CLOSE',
                color='SYMBOL',
                title='Stock Price Over Time'
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
            
            # Performance table
            st.markdown("#### Latest Performance")
            latest = data.groupby('SYMBOL').first().reset_index()
            st.dataframe(
                latest[['SYMBOL', 'COMPANY_NAME', 'CLOSE', 'DAILY_RETURN', 'ALPHA_VS_SP500', 'MARKET_CAP_CATEGORY']],
                use_container_width=True
            )
        else:
            st.warning("No data found for selected filters")
    else:
        st.error("No stock data available. Please run the ETL pipeline first.")

elif page == "📈 Benchmark Analysis":
    st.title("📈 Benchmark Analysis")
    
    # Stock selector
    stocks_df = run_query("SELECT DISTINCT symbol, company_name FROM COBRA_analytics.dim_stocks")
    
    if not stocks_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            selected_stock = st.selectbox(
                "Select Stock",
                stocks_df['SYMBOL'].tolist(),
                format_func=lambda x: f"{x} - {stocks_df[stocks_df['SYMBOL']==x]['COMPANY_NAME'].iloc[0]}"
            )
        
        with col2:
            date_range = st.slider(
                "Days to show",
                min_value=7,
                max_value=90,
                value=30
            )
        
        # Query data
        query = f"""
        SELECT 
            date,
            close,
            daily_return,
            sp500_value,
            sp500_return,
            alpha_vs_sp500,
            vix_value
        FROM COBRA_analytics.fact_daily_metrics
        WHERE symbol = '{selected_stock}'
        ORDER BY date DESC
        LIMIT {date_range}
        """
        
        data = run_query(query)
        
        if not data.empty:
            data = data.sort_values('DATE')
            
            # Price comparison
            st.markdown("#### Price vs S&P 500")
            fig = go.Figure()
            
            # Normalize to 100
            stock_norm = (data['CLOSE'] / data['CLOSE'].iloc[0]) * 100
            sp500_norm = (data['SP500_VALUE'] / data['SP500_VALUE'].iloc[0]) * 100
            
            fig.add_trace(go.Scatter(
                x=data['DATE'],
                y=stock_norm,
                name=selected_stock,
                line=dict(color='blue', width=2)
            ))
            
            fig.add_trace(go.Scatter(
                x=data['DATE'],
                y=sp500_norm,
                name='S&P 500',
                line=dict(color='red', width=2, dash='dash')
            ))
            
            fig.update_layout(
                title="Normalized Price Performance (Base 100)",
                yaxis_title="Index (100 = Start)",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Returns comparison
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### Cumulative Returns")
                cum_returns = (1 + data[['DAILY_RETURN', 'SP500_RETURN']].fillna(0)).cumprod() - 1
                
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(
                    x=data['DATE'],
                    y=cum_returns['DAILY_RETURN'] * 100,
                    name=selected_stock
                ))
                fig2.add_trace(go.Scatter(
                    x=data['DATE'],
                    y=cum_returns['SP500_RETURN'] * 100,
                    name='S&P 500'
                ))
                fig2.update_layout(yaxis_title="Return (%)", height=300)
                st.plotly_chart(fig2, use_container_width=True)
            
            with col2:
                st.markdown("#### VIX (Volatility Index)")
                fig3 = px.line(data, x='DATE', y='VIX_VALUE')
                fig3.update_layout(yaxis_title="VIX Level", height=300)
                st.plotly_chart(fig3, use_container_width=True)
            
            # Statistics
            st.markdown("#### Performance Statistics")
            stats_col1, stats_col2, stats_col3 = st.columns(3)
            
            with stats_col1:
                st.metric(
                    "Avg Daily Return",
                    f"{data['DAILY_RETURN'].mean() * 100:.2f}%"
                )
            
            with stats_col2:
                st.metric(
                    "Avg Alpha vs S&P 500",
                    f"{data['ALPHA_VS_SP500'].mean() * 100:.2f}%"
                )
            
            with stats_col3:
                st.metric(
                    "Volatility (Std Dev)",
                    f"{data['DAILY_RETURN'].std() * 100:.2f}%"
                )
        else:
            st.warning("No data available for selected stock")
    else:
        st.error("No stock data available")

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("### 📌 About")
st.sidebar.info("""
**Finance Analysis Pipeline**

Built with:
- Yahoo Finance (yfinance)
- Snowflake
- dbt
- Apache Airflow
- Streamlit
""")

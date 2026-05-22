import os
import streamlit as st
import altair as alt
import pandas as pd

st.set_page_config(page_title="Mag 7 Dashboard", layout="wide")
conn = st.connection("snowflake", ttl=os.getenv("SNOWFLAKE_CONNECTION_TTL"))

st.title("Magnificent 7: Dollar Dashboard")
st.markdown("Real-time stock prices converted to **Dollars**, automatically updated daily.")
st.divider() 

query = """
    SELECT 
        STOCK_DATE, 
        STOCK_NAME, 
        (STOCK_HIGH * 0.92) AS EUR_PRICE 
    FROM finance_project.analytics.raw_stock_feed 
    WHERE STOCK_DATE >= '2026-01-01' 
    ORDER BY STOCK_DATE
"""
df = conn.query(query)

df['STOCK_DATE'] = pd.to_datetime(df['STOCK_DATE'])

selected_tickers = st.multiselect(
    "Select Companies to Compare:", 
    df['STOCK_NAME'].unique(), 
    default=['AAPL', 'TSLA', 'NVDA'] 
)

filtered_df = df[df['STOCK_NAME'].isin(selected_tickers)]

if not filtered_df.empty:
    st.subheader("Year-to-Date Price Trends (€)")
    
    base = alt.Chart(filtered_df).encode(
        x=alt.X('STOCK_DATE:T', title='Date', axis=alt.Axis(format='%b %d, %Y', labelAngle=-45)),
        y=alt.Y('EUR_PRICE:Q', title='Price in Dollars ($)'),
        color=alt.Color('STOCK_NAME:N', title='Company'),
        tooltip=[
            'STOCK_NAME:N',
            alt.Tooltip('STOCK_DATE:T', format='%B %d, %Y', title='Date'),
            alt.Tooltip('EUR_PRICE:Q', format=',.2f', title='Price (€)')
        ]
    )
    lines = base.mark_line(strokeWidth=2)
    points = base.mark_circle(size=10)
    
    chart = (lines + points).properties(height=550).interactive()
    
    st.altair_chart(chart, use_container_width=True)
else:
    st.warning("Please select at least one company from the dropdown.")
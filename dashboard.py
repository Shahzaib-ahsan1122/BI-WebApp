import os
import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector
from dotenv import load_dotenv

#loading environemnt variables
load_dotenv()

# streamlit page configuration
st.set_page_config(
    page_title = "E-Commerce Real Time Data Pipeline",
    page_icon = "⚡",
    layout =  "wide"
)

st.title("⚡ Real Time E-Commerce Analytics Dashboard")
st.caption("Live streaming analytics powered by Snowflake Medallion Architecture (God Layer)")

# function to cnnect to snowflake

@st.cache_resource
def init_snowflake():
    # Sanitize account identifier (remove https:// if present)
    account = os.getenv("SNOWFLAKE_ACCOUNT", "").replace("https://", "").replace(".snowflakecomputing.com", "")
    
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=account,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "ECOM_PIPELINE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        client_session_keep_alive=True
    )

# fetch data from snowflake
def fetch_gold_data():
    conn = init_snowflake()
    query = """
    select
        product_name,
        total_orders,
        total_revenue,
        avg_order_value,
        last_purchase_at
    from gold_product_performance
    order by total_revenue desc;
    """
    return pd.read_sql(query, conn)

# refresh button
if st.button("🔄Refresh Data"):
    st.cache_data.clear()



try:
    df = fetch_gold_data()

    if df.empty:

        st.warning("No data found. ")
    else:

        # high level matrics
        total_revenue = df['TOTAL_REVENUE'].sum()
        total_orders =df['TOTAL_ORDERS'].sum()
        avg_aov = df['AVG_ORDER_VALUE'].mean()


        col1, col2, col3 = st.columns(3)
        col1.metric("Total Revenue" ,f"${total_revenue:,.2f}")
        col2.metric("Total Orders", f"${total_orders:,.2f}")
        col3.metric("Avg Order Value", f"${avg_aov:,.2f}")

        st.divider()


        # visualizations

        left_chart, right_chart =st.columns(2)

        with left_chart:
            st.subheader("Revenue by Product")
            fig_bar = px.bar(
                df,
                x = "PRODUCT_NAME",
                y = "TOTAL_REVENUE",
                color = "PRODUCT_NAME",
                text_auto = ".2s",
                title = "TOTAL REVENUE ($)"
            )
            st.plotly_chart(fig_bar, use_container_width=True)


        with right_chart:
            st.subheader("Order Volume by Product")
            fig_pie =px.pie(
                df,
                values = "TOTAL_ORDERS",
                names = "PRODUCT_NAME",
                title = "ORDER BREAKDOWN"
            )
            st.plotly_chart(fig_pie, use_container_width= True)

        # Data table view

        st.subheader("Raw Gold Table Data")
        st.dataframe(df, use_container_width=True)

except Exception as e:
    st.error(f"Error Connecting to Snowflake: {e}")
    
            
                
            
        


        

        

        

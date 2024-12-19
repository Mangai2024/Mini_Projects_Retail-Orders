import streamlit as st
import pandas as pd
import pg8000



# Function to connect to the PostgreSQL database
def get_db_connection():
    conn = pg8000.connect(
        host="dbmangai-1.cdig08ycykxi.ap-south-1.rds.amazonaws.com",
        port=5432,
        database="mangaidatabase",
        user="postgres",
        password="Rootawsroot"
    )
    
    return conn
# Function to execute a query and return the result as a pandas DataFrame
def run_query(query):
    conn = get_db_connection()
    if conn is None:
        return None
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return None
    finally:
        conn.close()

# Streamlit UI
st.title("Retail Order Dashboard")

# Split queries into two sections
queries_by_guvi = {
    "Top 10 highest revenue generating products":
        'SELECT t2.product_id, t2.sub_category, SUM(CAST(t2.sale_price AS FLOAT8) * CAST(t2.quantity AS FLOAT8)) AS total_revenue FROM table_1 t1 JOIN table_2 t2 ON t1.order_id = t2.order_id GROUP BY t2.product_id, t2.sub_category ORDER BY total_revenue DESC LIMIT 10;',
        
    "Top 5 cities with the highest profit margins":
        'WITH city_profit AS (SELECT t1.city, SUM(CAST(t2.sale_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC)) AS total_revenue, SUM(CAST(t2.cost_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC)) AS total_cost, CASE WHEN SUM(CAST(t2.sale_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC)) = 0 THEN 0 ELSE (SUM(CAST(t2.sale_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC)) - SUM(CAST(t2.cost_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC))) / SUM(CAST(t2.sale_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC)) END AS profit_margin FROM table_1 t1 JOIN table_2 t2 ON t1.order_id = t2.order_id GROUP BY t1.city) SELECT city, total_revenue, total_cost, profit_margin FROM city_profit ORDER BY profit_margin DESC LIMIT 5;',

    "Total discount given for each category": 
        'WITH category_discount AS (SELECT t2.sub_category, SUM(CAST(t2.sale_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC) * CAST(t2.discount AS NUMERIC)) AS total_discount FROM table_2 t2 GROUP BY t2.sub_category) SELECT sub_category, total_discount FROM category_discount ORDER BY total_discount DESC;',
    "Average sales price per product category": 
        'SELECT t2.sub_category, AVG(CAST(t2.sale_price AS NUMERIC)) AS average_sale_price FROM table_2 t2 JOIN table_1 t1 ON t1.order_id = t2.order_id GROUP BY t2.sub_category ORDER BY average_sale_price DESC;',

    "The highest average sale price":
        'SELECT t1.region, AVG(CAST(t2.sale_price AS NUMERIC)) AS average_sale_price FROM table_2 t2 JOIN table_1 t1 ON t1.order_id = t2.order_id GROUP BY t1.region ORDER BY average_sale_price DESC LIMIT 1;',

    "Total profit per category": 
        'SELECT t2.sub_category, SUM(CAST(t2.sale_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC)) AS total_profit FROM table_2 t2 JOIN table_1 t1 ON t1.order_id = t2.order_id GROUP BY t2.sub_category ORDER BY total_profit DESC;',

    "Top 3 segments with the highest quantity of orders": 
        'SELECT t2.sub_category, SUM(CAST(t2.quantity AS NUMERIC)) AS total_quantity FROM table_2 t2 JOIN table_1 t1 ON t1.order_id = t2.order_id GROUP BY t2.sub_category ORDER BY total_quantity DESC LIMIT 3;',

    "Average discount percentage given per region": 
        'SELECT t1.region, AVG(CAST(t2.discount AS NUMERIC)) AS average_discount_percentage FROM table_2 t2 JOIN table_1 t1 ON t1.order_id = t2.order_id GROUP BY t1.region ORDER BY average_discount_percentage DESC;',

    "Product category with the highest total profit": 
        'SELECT t2.sub_category, SUM(CAST(t2.sale_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC)) AS total_profit FROM table_2 t2 JOIN table_1 t1 ON t1.order_id = t2.order_id GROUP BY t2.sub_category ORDER BY total_profit DESC LIMIT 1;',

    "Total revenue generated per year": 
        'SELECT EXTRACT(YEAR FROM t1.order_date) AS year, SUM(CAST(t2.sale_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC)) AS total_revenue FROM table_2 t2 JOIN table_1 t1 ON t1.order_id = t2.order_id GROUP BY year ORDER BY year DESC;'
}

my_queries = {
    "-- Top 10 Revenue Products by Region": 
        'SELECT region, sub_category, total_revenue FROM (SELECT t1.region AS region, t2.sub_category AS sub_category, SUM(CAST(t2.sale_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC)) AS total_revenue FROM table_1 t1 JOIN table_2 t2 ON t1.order_id = t2.order_id GROUP BY t1.region, t2.sub_category) AS subquery ORDER BY total_revenue DESC LIMIT 10;',

    "-- Highest Revenue by products": 
        'SELECT t2.product_id, t2.sub_category, t1.city, t1.state, SUM(CAST(t2.sale_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC)) AS total_revenue FROM table_1 t1 JOIN table_2 t2 ON t1.order_id = t2.order_id GROUP BY t2.product_id, t2.sub_category, t1.city, t1.state ORDER BY total_revenue DESC;',

    "-- Determine the city that generates the highest revenue for each product":
        'SELECT CityRevenue.product_id, CityRevenue.sub_category, CityRevenue.city, CityRevenue.state, CityRevenue.total_revenue FROM (SELECT t2.product_id, t2.sub_category, t1.city, t1.state, SUM(CAST(t2.sale_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC)) AS total_revenue FROM table_1 t1 JOIN table_2 t2 ON t1.order_id = t2.order_id GROUP BY t2.product_id, t2.sub_category, t1.city, t1.state) AS CityRevenue JOIN (SELECT product_id, sub_category, city, state, total_revenue, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY total_revenue DESC) AS rank FROM (SELECT t2.product_id, t2.sub_category, t1.city, t1.state, SUM(CAST(t2.sale_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC)) AS total_revenue FROM table_1 t1 JOIN table_2 t2 ON t1.order_id = t2.order_id GROUP BY t2.product_id, t2.sub_category, t1.city, t1.state) AS CityRevenue) AS HighestRevenueCities ON CityRevenue.product_id = HighestRevenueCities.product_id WHERE HighestRevenueCities.rank = 1 ORDER BY total_revenue DESC;',

    "-- Find the top 5 most profitable products for each customer segment": 
        'SELECT psp.segment, psp.product_id, psp.sub_category, psp.total_profit FROM (SELECT t1.segment, t2.product_id, t2.sub_category, SUM(t2.profit) AS total_profit FROM table_1 t1 JOIN table_2 t2 ON t1.order_id = t2.order_id GROUP BY t1.segment, t2.product_id, t2.sub_category) AS psp JOIN (SELECT t1.segment, t2.product_id, t2.sub_category, SUM(t2.profit) AS total_profit, ROW_NUMBER() OVER (PARTITION BY t1.segment ORDER BY SUM(t2.profit) DESC) AS rank FROM table_1 t1 JOIN table_2 t2 ON t1.order_id = t2.order_id GROUP BY t1.segment, t2.product_id, t2.sub_category) AS rp ON psp.product_id = rp.product_id WHERE rp.rank <= 5 ORDER BY psp.segment, psp.total_profit DESC;',
    "-- Analyze the correlation between discounts given and sales revenue in different regions": 
        'SELECT region, avg_discount, total_sales FROM (SELECT t1.region, AVG(t2.discount_percent) AS avg_discount, SUM(CAST(t2.sale_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC)) AS total_sales FROM table_1 t1 JOIN table_2 t2 ON t1.order_id = t2.order_id GROUP BY t1.region) AS RegionalDiscountSales ORDER BY avg_discount DESC;',
    "-- Total Revenue and Profit by Year and Category":
       'SELECT t1.category, SUM(CAST(t2.sale_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC)) AS total_revenue, SUM(CAST(t2.profit AS NUMERIC)) AS total_profit FROM table_1 t1 JOIN table_2 t2 ON t1.order_id = t2.order_id GROUP BY t1.category ORDER BY total_revenue DESC;',
    "-- Determine the average discount offered for each product across different regions":
        'SELECT t2.product_id, t2.sub_category, t1.region, ROUND(AVG(t2.discount_percent), 2) AS avg_discount FROM table_1 t1 JOIN table_2 t2 ON t1.order_id = t2.order_id GROUP BY t2.product_id, t2.sub_category, t1.region ORDER BY region, avg_discount DESC;',
    " -- High-Volume Cities for Top Products": 
        'SELECT pcs.product_id, pcs.sub_category, pcs.city, pcs.state, pcs.total_quantity FROM (SELECT t2.product_id, t2.sub_category, t1.city, t1.state, SUM(CAST(t2.quantity AS NUMERIC)) AS total_quantity FROM table_1 t1 JOIN table_2 t2 ON t1.order_id = t2.order_id GROUP BY t2.product_id, t2.sub_category, t1.city, t1.state) AS pcs JOIN (SELECT product_id, sub_category, SUM(total_quantity) AS overall_quantity FROM (SELECT t2.product_id, t2.sub_category, t1.city, t1.state, SUM(CAST(t2.quantity AS NUMERIC)) AS total_quantity FROM table_1 t1 JOIN table_2 t2 ON t1.order_id = t2.order_id GROUP BY t2.product_id, t2.sub_category, t1.city, t1.state) AS ProductCitySales GROUP BY product_id, sub_category ORDER BY overall_quantity DESC LIMIT 10) AS tp ON pcs.product_id = tp.product_id ORDER BY pcs.product_id, pcs.total_quantity DESC;',
    "-- Identify the cities with the highest order quantities for the top 3 revenue-generating products": 
        'SELECT coq.product_id, coq.city, coq.state, coq.total_quantity FROM (SELECT t2.product_id, t1.city, t1.state, SUM(CAST(t2.quantity AS NUMERIC)) AS total_quantity FROM table_1 t1 JOIN table_2 t2 ON t1.order_id = t2.order_id GROUP BY t2.product_id, t1.city, t1.state) coq JOIN (SELECT t2.product_id, t2.sub_category, SUM(CAST(t2.sale_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC)) AS total_revenue FROM table_1 t1 JOIN table_2 t2 ON t1.order_id = t2.order_id GROUP BY t2.product_id, t2.sub_category ORDER BY total_revenue DESC LIMIT 3) ProductRevenue ON coq.product_id = ProductRevenue.product_id ORDER BY coq.product_id, coq.total_quantity DESC;',
    "-- Category-Wise Sales Growth by Year": 
        'SELECT CategoryYearlySales1.category, CategoryYearlySales1.year, CategoryYearlySales1.total_revenue AS revenue_current_year, CategoryYearlySales1.total_revenue - COALESCE(CategoryYearlySales2.total_revenue, 0) AS revenue_growth FROM (SELECT EXTRACT(YEAR FROM t1.order_date::DATE) AS year, t1.category, SUM(CAST(t2.sale_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC)) AS total_revenue FROM table_1 t1 JOIN table_2 t2 ON t1.order_id = t2.order_id GROUP BY year, t1.category) CategoryYearlySales1 LEFT JOIN (SELECT EXTRACT(YEAR FROM t1.order_date::DATE) AS year, t1.category, SUM(CAST(t2.sale_price AS NUMERIC) * CAST(t2.quantity AS NUMERIC)) AS total_revenue FROM table_1 t1 JOIN table_2 t2 ON t1.order_id = t2.order_id GROUP BY year, t1.category) CategoryYearlySales2 ON CategoryYearlySales1.category = CategoryYearlySales2.category AND CategoryYearlySales1.year = CategoryYearlySales2.year + 1 ORDER BY CategoryYearlySales1.category, CategoryYearlySales1.year;',




}

# Navigation options
nav = st.radio("Select Query Section", ["Queries by GUVI", "My Queries"])

# Query selection based on navigation
if nav == "Queries by GUVI":
    st.subheader("Queries by GUVI")
    query = st.selectbox("Select a query to visualize:", list(queries_by_guvi.keys()))
    selected_query_set = queries_by_guvi
elif nav == "My Queries":
    st.subheader("My Queries")
    query = st.selectbox("Select a query to visualize:", list(my_queries.keys()))
    selected_query_set = my_queries
else:
    query = None

# Execute and visualize selected query
if query:
    result_df = run_query(selected_query_set[query])
    if result_df is not None:
        st.dataframe(result_df)

st.text("Thank you for using the dashboard!")

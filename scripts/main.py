import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd

# Load environment variables
load_dotenv()

# Get credentials from .env
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

# Query + export
query = {
  "dim_customer" : "SELECT * FROM gold.dim_customer",
  "dim_products" : "SELECT * FROM gold.dim_products",
  "fact_sales" : "SELECT * FROM gold.fact_sales"
}
for name, query in query.items():
  df = pd.read_sql(query, conn)
  df.to_csv(f"{name}.csv", index = False)
  print(f"Exported {name}.csv!");
conn.close()

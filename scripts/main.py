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
query = "SELECT * FROM gold.report_customers"
df = pd.read_sql(query, conn)
df.to_csv("report_customers.csv", index=False)

print("✅ Exported report_customers.csv")
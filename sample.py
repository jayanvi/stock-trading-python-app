from dotenv import load_dotenv
import os, snowflake.connector

load_dotenv()

print("Account:", os.getenv("SNOWFLAKE_ACCOUNT"))

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT")
)

print("✅ Connection successful!")
conn.close()

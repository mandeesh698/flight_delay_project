from snowflake.connector import connect
import os, pandas as pd

conn = connect(
    user=os.getenv("SF_USER"),
    password=os.getenv("SF_PASSWORD"),
    account=os.getenv("SF_ACCOUNT"),
    warehouse=os.getenv("SF_WAREHOUSE"),
    database=os.getenv("SF_DATABASE"),
    schema=os.getenv("SF_SCHEMA")
)
cur = conn.cursor()
# Create stage + table if needed
cur.execute("CREATE OR REPLACE TABLE FLIGHT_CURATED (distance_km NUMBER, dep_hour NUMBER, day_of_week NUMBER, weather_score FLOAT, carrier_on_time_rate FLOAT, origin_traffic FLOAT, season NUMBER, delayed NUMBER);")
# Use PUT/COPY or insert via snowflake-sqlalchemy
df = pd.read_csv("data/processed/flight_processed.csv")
for _, row in df.iterrows():
    cur.execute("INSERT INTO FLIGHT_CURATED VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", tuple(row))
cur.close()
conn.close()

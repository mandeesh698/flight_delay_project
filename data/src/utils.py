# src/utils.py
import os
import io
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ---------- AWS S3 helpers ----------
def get_s3_client():
    """
    Expects AWS credentials in env:
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
    """
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )

def upload_file_to_s3(local_path, bucket, s3_key):
    s3 = get_s3_client()
    try:
        s3.upload_file(local_path, bucket, s3_key)
        print(f"Uploaded {local_path} to s3://{bucket}/{s3_key}")
        return True
    except ClientError as e:
        print("S3 upload error:", e)
        return False

def upload_dataframe_to_s3(df: pd.DataFrame, bucket, s3_key, index=False):
    s3 = get_s3_client()
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=index)
    try:
        s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key=s3_key)
        print(f"Uploaded dataframe to s3://{bucket}/{s3_key}")
        return True
    except ClientError as e:
        print("S3 upload error:", e)
        return False

def download_file_from_s3(bucket, s3_key, local_path):
    s3 = get_s3_client()
    try:
        s3.download_file(bucket, s3_key, local_path)
        print(f"Downloaded s3://{bucket}/{s3_key} to {local_path}")
        return True
    except ClientError as e:
        print("S3 download error:", e)
        return False

# ---------- Snowflake helpers ----------
def get_snowflake_conn():
    """
    Expects Snowflake connection info in environment variables:
    SF_USER, SF_PASSWORD, SF_ACCOUNT, SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA
    """
    ctx = snowflake.connector.connect(
        user=os.getenv("SF_USER"),
        password=os.getenv("SF_PASSWORD"),
        account=os.getenv("SF_ACCOUNT"),
        warehouse=os.getenv("SF_WAREHOUSE"),
        database=os.getenv("SF_DATABASE"),
        schema=os.getenv("SF_SCHEMA"),
        role=os.getenv("SF_ROLE") or None
    )
    return ctx

def write_df_to_snowflake(df: pd.DataFrame, table_name: str, if_exists="append"):
    """
    Uses write_pandas to efficiently load a pandas DataFrame to Snowflake.
    if_exists: 'append' or 'replace'
    """
    ctx = get_snowflake_conn()
    cs = ctx.cursor()
    try:
        if if_exists.lower() == "replace":
            cs.execute(f"CREATE OR REPLACE TABLE {table_name} (\
                distance_km FLOAT, dep_hour INT, day_of_week INT, weather_score FLOAT, \
                carrier_on_time_rate FLOAT, origin_traffic FLOAT, season INT, delayed INT)")
        success, nchunks, nrows, _ = write_pandas(ctx, df, table_name.upper())
        print(f"write_pandas success={success}, rows={nrows}, chunks={nchunks}")
        return success
    except Exception as e:
        print("Snowflake write error:", e)
        raise
    finally:
        cs.close()
        ctx.close()

def run_query(sql: str):
    ctx = get_snowflake_conn()
    try:
        cur = ctx.cursor()
        cur.execute(sql)
        try:
            res = cur.fetchall()
            cols = [c[0] for c in cur.description]
            df = pd.DataFrame(res, columns=cols)
            return df
        except Exception:
            return None
    finally:
        cur.close()
        ctx.close()

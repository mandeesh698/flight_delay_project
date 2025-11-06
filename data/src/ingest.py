import requests
import pandas as pd
import time
from datetime import datetime
import boto3  # optional: to upload to S3

OPENSKY_URL = "https://opensky-network.org/api/flights/aircraft?icao24=<icao>&begin=<start>&end=<end>"

def fetch_opensky(params):
    resp = requests.get(OPENSKY_URL, params=params)
    data = resp.json()
    df = pd.json_normalize(data)
    return df

# Example: save locally
df.to_csv("data/raw/flights_{}.csv".format(datetime.utcnow().strftime("%Y%m%d%H%M")) , index=False)

# Optional S3 upload:
s3 = boto3.client("s3")
s3.upload_file("path/to/file.csv", "my-bucket", "raw/flights/file.csv")

import pandas as pd
def transform(infile, outfile):
    df = pd.read_csv(infile)
    # sample feature engineering:
    df['dep_hour'] = pd.to_datetime(df['scheduled_departure']).dt.hour
    df['day_of_week'] = pd.to_datetime(df['scheduled_departure']).dt.dayofweek
    df['is_evening'] = (df['dep_hour'] >= 17).astype(int)
    # compute target: delayed > 15 mins
    df['delay_minutes'] = (pd.to_datetime(df['actual_arrival']) - pd.to_datetime(df['scheduled_arrival'])).dt.total_seconds() / 60
    df['delayed'] = (df['delay_minutes'] > 15).astype(int)
    # keep only needed features
    out = df[['distance_km','dep_hour','day_of_week','weather_score','carrier_on_time_rate','origin_traffic','season','delayed']]
    out.to_csv(outfile, index=False)

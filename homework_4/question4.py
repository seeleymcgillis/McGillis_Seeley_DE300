from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import boto3
import pandas as pd
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
import io
from sklearn.preprocessing import OneHotEncoder

S3_BUCKET_NAME = "mcgillis-wu-beam-smith-mwaa"
INPUT_DIR = "weather_data"
OUTPUT_DIR = "predictions"

#Define the default args dictionary
default_args = {
    'owner': 'mcgillis-wu-beam-smith',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def combine_all_data():
    #Look for todays date
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=INPUT_DIR + "/")

    #Read relevant files
    dfs = []
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".csv"):
            file_obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
            df = pd.read_csv(io.BytesIO(file_obj['Body'].read()))
            dfs.append(df)
        #if today in key and key.endswith(".csv"):

    #Save to tmp local file
    if dfs:
        combined_df = pd.concat(dfs)
        buffer = io.StringIO()
        combined_df.to_csv(buffer, index=False)
        buffer.seek(0)
        s3.put_object(Bucket=S3_BUCKET_NAME, Key="preprocessed/all_weather.csv", Body=buffer.getvalue())
    else:
        print("No data to combine.")

def lin_reg():
    today = datetime.utcnow().strftime('%Y%m%d')
    s3 = boto3.client("s3")
    S3_BUCKET_NAME = "mcgillis-wu-beam-smith-mwaa"
    OUTPUT_DIR = "predictions"

    # Load data
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key="preprocessed/all_weather.csv")
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))

    if df.empty:
        raise ValueError("DataFrame is empty. Check your data collection.")

    # Validate required columns
    required_columns = {'station', 'timestamp', 'temperature'}
    if not required_columns.issubset(df.columns):
        raise KeyError(f"Missing columns: {required_columns - set(df.columns)}")

    # Parse timestamp
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df.dropna(subset=['timestamp'], inplace=True)

    # Filter for at least 20 hours of data
    min_time = df['timestamp'].min()
    max_time = df['timestamp'].max()
    time_span_hours = (max_time - min_time).total_seconds() / 3600
    if time_span_hours < 20:
        raise ValueError("Not enough data collected yet (need at least 20 hours).")

    # Feature engineering
    df['hour'] = df['timestamp'].dt.hour
    df['dayofweek'] = df['timestamp'].dt.dayofweek

    # One-hot encode 'station'
    enc = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
    station_encoded = enc.fit_transform(df[['station']])
    station_df = pd.DataFrame(station_encoded, columns=enc.get_feature_names_out(['station']))
    df = pd.concat([df.reset_index(drop=True), station_df], axis=1)

    # Select features and target
    features = ['hour', 'dayofweek'] + list(station_df.columns)
    target = 'temperature'
    df.dropna(subset=[target], inplace=True)
    X = df[features]
    y = df[target]

    # Train model
    model = LinearRegression()
    model.fit(X, y)

    # Predict next 8 hours in 30-min steps
    stations = df['station'].unique()
    future_timestamps = pd.date_range(start=max_time, periods=17, freq='30T')[1:]

    predictions = []
    for ts in future_timestamps:
        hour = ts.hour
        dayofweek = ts.dayofweek
        for station in stations:
            row = {'hour': hour, 'dayofweek': dayofweek, 'station': station}
            row_df = pd.DataFrame([row])
            encoded = enc.transform(row_df[['station']])
            encoded_df = pd.DataFrame(encoded, columns=enc.get_feature_names_out(['station']))
            row_df = pd.concat([row_df.drop(columns=['station']), encoded_df], axis=1)
            row_df = row_df.reindex(columns=features, fill_value=0)
            pred_temp = model.predict(row_df)[0]
            predictions.append({
                'timestamp': ts,
                'station': station,
                'predicted_temperature': pred_temp
            })

    # Save and upload predictions
    pred_df = pd.DataFrame(predictions)
    filename = f"predictions_{today}.csv"
    filepath = f"/tmp/{filename}"
    pred_df.to_csv(filepath, index=False)
    with open(filepath, "rb") as f:
        s3.upload_fileobj(f, S3_BUCKET_NAME, f"{OUTPUT_DIR}/{filename}")


#Define DAG
dag = DAG(
    dag_id='jw_linear_regression_models',
    description='Generates predictions of weather data from S3',
    schedule_interval=timedelta(hours=20),
    start_date=pendulum.today('UTC').add(hours=-20),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=['weather', 'dashboard', 's3'],
)

#Define tasks
task_combine_all_data = PythonOperator(
    task_id='combine_all_data',
    python_callable=combine_all_data,
    dag=dag,
)
task_lin_reg = PythonOperator(
    task_id='Make_Linear_Regression',
    python_callable=lin_reg,
    dag=dag,
)

task_combine_all_data >> task_lin_reg
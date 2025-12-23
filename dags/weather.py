from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import urllib.request
import urllib.parse
import json
import os

DATA_DIR = "/opt/airflow/logs"

LAT = 35.6944
LON = 51.4215

HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "wind_speed_10m",
    "wind_direction_10m"
]

def fetch_openmeteo():
    params = {
        "latitude": LAT,
        "longitude": LON,
        "hourly": ",".join(HOURLY_VARS),
        "forecast_days": 1,
        "timezone": "UTC"
    }

    query_string = urllib.parse.urlencode(params)
    url = f"https://api.open-meteo.com/v1/forecast?{query_string}"

    with urllib.request.urlopen(url, timeout=30) as response:
        raw_data = response.read().decode("utf-8")

    data = json.loads(raw_data)

    times = data["hourly"]["time"]
    hourly_data = []

    for i, t in enumerate(times):
        row = {"datetime": t}
        for var in HOURLY_VARS:
            row[var] = data["hourly"][var][i]
        hourly_data.append(row)

    os.makedirs(DATA_DIR, exist_ok=True)
    output_path = f"{DATA_DIR}/openmeteo_{datetime.now()}.json"

    with open(output_path, "w") as f:
        json.dump(hourly_data, f, indent=2)

    print(f"Saved {len(hourly_data)} records to {output_path}")


def count_files():
    if not os.path.exists(DATA_DIR):
        print("Data directory does not exist")
        return 0

    count = sum(
        1 for f in os.listdir(DATA_DIR)
        if os.path.isfile(os.path.join(DATA_DIR, f))
    )

    print(f"Total files in {DATA_DIR}: {count}")
    return count


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="openmeteo_weather_stdlib",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    tags=["weather", "stdlib"],
) as dag:

    fetch_weather = PythonOperator(
        task_id="fetch_openmeteo_weather",
        python_callable=fetch_openmeteo
    )

    count_weather_files = PythonOperator(
        task_id="count_weather_files",
        python_callable=count_files
    )

    fetch_weather >> count_weather_files

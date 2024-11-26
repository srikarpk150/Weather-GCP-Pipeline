#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests
import pandas as pd
import os
from google.cloud import storage
from google.cloud import bigquery
import datetime
import json
import config


def fetch_data_from_url(api_endpoint: str) -> dict:
    """Retrieve data from a provided API endpoint."""
    response = requests.get(api_endpoint)
    response.raise_for_status()
    return response.json()


def collect_weather_info(locations: dict, api_key: str) -> dict:
    """
    Gather weather information for all specified locations.
    
    Args:
        locations: A dictionary with location details (latitude, longitude).
        api_key: API key for OpenWeatherMap.
    
    Returns:
        A dictionary with current and forecasted weather details.
    """
    api_base = "https://api.openweathermap.org/data/2.5/"
    weather_details = {"current": {}, "forecast": {}}
    
    for city, coordinates in locations.items():
        try:
            # API URLs
            current_weather_url = f"{api_base}weather?lat={coordinates['lat']}&lon={coordinates['lon']}&appid={api_key}&units=metric"
            forecast_weather_url = f"{api_base}forecast?lat={coordinates['lat']}&lon={coordinates['lon']}&appid={api_key}&units=metric"
            
            # Fetch data and update dictionary
            weather_details["current"][city] = fetch_data_from_url(current_weather_url)
            weather_details["forecast"][city] = fetch_data_from_url(forecast_weather_url)
        except requests.exceptions.RequestException as error:
            print(f"Unable to retrieve data for {city}: {error}")
    
    return weather_details


def process_current_weather(weather_data: dict) -> pd.DataFrame:
    """
    Format current weather data into a structured DataFrame.
    
    Args:
        weather_data: Raw current weather data.
    
    Returns:
        A DataFrame with processed weather information.
    """
    if isinstance(weather_data['weather'], list):
        weather_data['weather'] = weather_data['weather'][0]

    flat_data = {}
    for key, value in weather_data.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                flat_data[f"{key}_{sub_key}"] = sub_value
        else:
            flat_data[key] = value

    weather_df = pd.DataFrame([flat_data])
    if 'dt' in weather_df.columns:
        weather_df['timestamp'] = pd.to_datetime(weather_df['dt'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')

    return weather_df


def structure_forecast_data(forecast_data: dict) -> pd.DataFrame:
    """
    Convert forecast data into a structured format.
    
    Args:
        forecast_data: Raw forecasted weather data.
    
    Returns:
        A DataFrame with detailed forecast information.
    """
    city_info = pd.DataFrame([forecast_data['city']])
    forecasts = pd.DataFrame(forecast_data['list'])
    
    # Expand weather details
    for i in range(len(forecasts)):
        forecasts.at[i, 'weather'] = forecasts.at[i, 'weather'][0]
    
    structured_forecast = forecasts.join(city_info, how='cross')
    return structured_forecast


def preprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess a DataFrame to flatten nested structures and ensure consistent types.
    
    Args:
        df: The input DataFrame.
    
    Returns:
        A cleaned and preprocessed DataFrame.
    """
    # Flatten nested columns if present
    for column in df.columns:
        if df[column].apply(lambda x: isinstance(x, dict)).any():
            expanded_cols = pd.json_normalize(df[column])
            expanded_cols.columns = [f"{column}_{sub_col}" for sub_col in expanded_cols.columns]
            df = df.drop(columns=[column]).join(expanded_cols)

    # Convert lists or mixed types to JSON strings
    for column in df.columns:
        if df[column].apply(lambda x: isinstance(x, (list, dict))).any():
            df[column] = df[column].apply(json.dumps)
    
    return df


def upload_to_cloud_storage(json_payload: dict, storage_bucket: str, folder: str) -> None:
    """
    Upload a JSON object to Google Cloud Storage.
    
    Args:
        json_payload: The JSON data to be uploaded.
        storage_bucket: Target storage bucket name.
        folder: Subfolder within the bucket for organizing files.
    """
    if config.is_local_environment:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.gcs_credentials

    client = storage.Client()
    try:
        bucket = client.get_bucket(storage_bucket)
    except:
        bucket = client.create_bucket(storage_bucket)

    filename = f"{datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.json"
    file_path = os.path.join(folder, filename)
    
    blob = bucket.blob(file_path)
    blob.upload_from_string(json.dumps(json_payload), content_type="application/json")
    print(f"Uploaded: {filename} to {storage_bucket}/{folder}")


def push_to_bigquery(data: pd.DataFrame, project: str, dataset: str, table: str):
    """
    Push a DataFrame to a BigQuery table.
    
    Args:
        data: DataFrame to upload.
        project: GCP project identifier.
        dataset: BigQuery dataset name.
        table: BigQuery table name.
    """
    if config.is_local_environment:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.bq_credentials

    client = bigquery.Client()
    dataset_id = f"{project}.{dataset}"
    
    try:
        client.create_dataset(bigquery.Dataset(dataset_id), timeout=30)
    except:
        pass 
    
    table_id = f"{dataset_id}.{table}"
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED'
    )

    data = preprocess_dataframe(data)

    job = client.load_table_from_dataframe(data, table_id, job_config=job_config)
    job.result()
    print(f"Uploaded data to BigQuery: {table_id}")


def main(request: dict) -> str:
    """
    Handle weather data collection, transformation, and storage.
    
    Args:
        request: Incoming request containing necessary parameters.
    
    Returns:
        Status message indicating success.
    """
    try:
        request_content = request.get_json()
    except:
        request_content = json.loads(request)
    
    api_key = config.api_key

    locations = {
        'Hyderabad, IN': {'lat': '17.4065', 'lon': '78.4772'},
        'London, GB': {'lat': '51.50853', 'lon': '-0.12574'},
        'Dubai, AE': {'lat': '25.276987', 'lon': '55.296249'},
        'kolkata, IN': {'lat': '22.5744', 'lon': '88.3629'},
        'Bloomington, US': {'lat': '39.1653', 'lon': '-86.5366'}
    }

    weather_info = collect_weather_info(locations, api_key)
    current_weather_df = pd.concat(
        [process_current_weather(data) for data in weather_info['current'].values()], ignore_index=True
    )
    forecast_weather_df = pd.concat(
        [structure_forecast_data(data) for data in weather_info['forecast'].values()], ignore_index=True
    )

    bucket = config.bucket_name
    
    for city, data in weather_info['current'].items():
        upload_to_cloud_storage(data, bucket, f"current/{city}")
    for city, data in weather_info['forecast'].items():
        upload_to_cloud_storage(data, bucket, f"forecast/{city}")

    push_to_bigquery(current_weather_df, config.project_id, config.dataset_id, 'current_weather')
    push_to_bigquery(forecast_weather_df, config.project_id, config.dataset_id, 'forecast_weather')

    return "Processing completed successfully."


if __name__ == "__main__":
    mock_request = {}
    print(main(json.dumps(mock_request)))

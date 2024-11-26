is_local_environment = True # Set to False before deploying to Cloud Functions
gcs_credentials= "cloud_storage_service_account.json" # Replace with your Cloud Storage service account filename
bq_credentials= "bigquery_service_account.json" # Replace with your BigQuery service account filename

api_key = '' # Replace with your OpenWeather API key
project_id = 'fa24-i535-vpothapr-weatherpipe' # Replace with your GCP Project ID
dataset_id= 'weather_api_data' # Replace with your BigQuery Dataset Name
bucket_name = 'weather-data-api-openweather' # Replace with your Cloud Storage Bucket Name
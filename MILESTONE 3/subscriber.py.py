import json
import os
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1, storage
from datetime import datetime, timedelta
import numpy as np
import csv
import tempfile
import pandas as pd
import psycopg2
import zipfile

# Project ID and Subscription ID
project_id = "data-engineering-420102"
subscription_id = "bussub"

# Google Cloud Storage bucket name
bucket_name = "busdata-archive"


# Database connection
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="sridhamo",
    host="localhost",
    port="5432"
)
# Opening the connection
cursor = conn.cursor()

# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# File for output data
today = datetime.now().strftime("%Y-%m-%d")
file_name = f"breadcrumb_{today}.json"

# Temp file header
header = ['VEHICLE_ID', 'EVENT_NO_TRIP', 'EVENT_NO_STOP', 'ACT_TIME', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'METERS','OPD_DATE','GPS_SATELLITES','GPS_HDOP']

processed_combinations = set()
first_operation_date = None

tempfile_path = f"breadcrumb_records_{today}.csv"

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def compress_and_upload_file(file_path, bucket_name):
    zip_file_path = f"{file_path}.zip"
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(file_path, os.path.basename(file_path))
    
    upload_to_gcs(bucket_name, zip_file_path, os.path.basename(zip_file_path))

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    json_message = json.loads(message.data.decode('utf-8'))
    
    latitude = json_message.get('GPS_LATITUDE', None)
    longitude = json_message.get('GPS_LONGITUDE', None)
    vehicle_id = json_message.get('VEHICLE_ID', None)
    trip_number = json_message.get('EVENT_NO_TRIP', None)
    stop_number = json_message.get('EVENT_NO_STOP', None)
    actual_time = json_message.get('ACT_TIME', None)
    operation_date = json_message.get('OPD_DATE', None)
    satellites = json_message.get('GPS_SATELLITES', None)
    hdop = json_message.get('GPS_HDOP', None)
    meters = json_message.get('METERS', None)
    
    # Assertion1: Check if trip number exists
    if 'EVENT_NO_TRIP' not in json_message:
        print("Error: Missing trip number in message")
        message.ack()
        return
    # Assertion2: Every trip record must have an operation date and actual time
    if 'OPD_DATE' not in json_message or 'ACT_TIME' not in json_message:
        print("Error: Missing operation date or actual time in message")
        message.ack()
        return

    # Assertion3: Every trip must have a stop number
    if 'EVENT_NO_STOP' not in json_message:
        print("Error: Missing stop number in message")
        message.ack()
        return
    # Assertion4: Check latitude range
    if latitude is not None and (latitude < 45.2 or latitude > 45.7):
        print("Error: Latitude value out of range")
        message.ack()
        return
    # Assertion5: Check longitude range
    if longitude is not None and (longitude < -124.0 or longitude > -122.0):
        print("Error: Longitude value out of range")
        message.ack()
        return

    # Assertion6: Check if combination of vehicle ID, trip number, stop number, and actual time is unique
    unique_combination = (vehicle_id, trip_number, stop_number, actual_time, meters)
    if unique_combination in processed_combinations:
        print(vehicle_id, trip_number, stop_number, actual_time, latitude, longitude, meters)
        print("Error: Duplicate combination of vehicle ID, trip number, stop number, and actual time")
        message.ack()
        return
    processed_combinations.add(unique_combination)
    
    # Assertion7: Check if meters value is negative
    meters = json_message.get('METERS', None)
    if meters is not None and meters < 0:
        print("Error: Meters value is negative")
        message.ack()
        return

    # Store the first operation date
    global first_operation_date
    if first_operation_date is None:
        first_operation_date = operation_date

    # Assertion8: For a particular day, all trip records will have the same operation date
    if operation_date != first_operation_date:
        print("Error: Operation date does not match the first operation date")
        message.ack()
        return

    # Assertion9: Every trip record must have a meters value
    if 'METERS' not in json_message:
        print("Error: Missing meters value in message")
        message.ack()
        return
   
    # Assertion10: Satellites must be between 0 and 12
    if latitude is None and longitude is None:
        print("Error: Latitude and longitude values do not exist")
        message.ack()
        return

    # Data transformation
    with open(tempfile_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        if os.stat(tempfile_path).st_size == 0:
            writer.writerow(header)
        writer.writerow([vehicle_id, trip_number, stop_number, actual_time, latitude, longitude, meters, operation_date, satellites, hdop])

print(f"Listening for messages on {subscription_path}..\n")

if __name__ == "__main__":
    while True:
        #with subscriber:
            streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
            try:
                streaming_pull_future.result(timeout=1000)
            except TimeoutError:
                print(f"Writing breadcrumb records to database.")
                df = pd.read_csv(tempfile_path)
                df['OPD_DATE'] = pd.to_datetime(df['OPD_DATE'], format='%d%b%Y:%H:%M:%S')

                # Insert into trip table 
                cursor.execute(
                    "INSERT INTO trip (trip_id, vehicle_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (df['EVENT_NO_TRIP'], df['VEHICLE_ID'])
                )
                conn.commit()
      
                # Group by EVENT_NO_TRIP
                group = df.groupby('EVENT_NO_TRIP')
                dfs = []

                for trip_id, data in group:
                    # Sort the data by ACT_TIME
                    data = data.sort_values(by='ACT_TIME')
                    data['TIMESTAMP'] = data.apply(lambda row: row['OPD_DATE'] + timedelta(seconds=row['ACT_TIME']), axis=1)

                    # Calculate distance and time difference
                    data['dMETERS'] = data['METERS'].diff()
                    data['dTIMESTAMP'] = data['TIMESTAMP'].diff()
                    # Fill the missing latitude and longitude values
                    data['GPS_LATITUDE'] = data['GPS_LATITUDE'].ffill()
                    data['GPS_LONGITUDE'] = data['GPS_LONGITUDE'].ffill()
                    # Calculate the speed
                    data['SPEED'] = data.apply(lambda row: row['dMETERS'] / row['dTIMESTAMP'].total_seconds() if row['dTIMESTAMP'].total_seconds() != 0 else 0, axis=1)

                    # Append the modified data to the list of DataFrames
                    dfs.append(data)

                # Concatenate the DataFrames
                df_concatenated = pd.concat(dfs)

                # Drop unnecessary columns
                df_concatenated = df_concatenated.drop(['dMETERS', 'dTIMESTAMP', 'GPS_SATELLITES', 'GPS_HDOP', 'OPD_DATE', 'ACT_TIME', 'VEHICLE_ID', 'EVENT_NO_STOP', 'METERS'], axis=1)

                # Use the second speed value for the first speed value
                df_concatenated['SPEED'] = df_concatenated['SPEED'].bfill()
                # Rearrange the columns to match the DB schema
                df_concatenated = df_concatenated[['TIMESTAMP', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED', 'EVENT_NO_TRIP']]
                # Calculate the timestamp
                df_concatenated['TIMESTAMP'] = pd.to_datetime(df_concatenated['TIMESTAMP'], format='%y-%m-%d %H:%M:%S')
                df_concatenated.to_csv("processed_breadcrumb_records.csv", index=False)
                
                # Insert into breadcrumb table
                for index, row in df_concatenated.iterrows():
                    cursor.execute(
                        "INSERT INTO breadcrumb (tstamp, latitude, longitude, speed, trip_id) VALUES (%s, %s, %s, %s, %s)",
                        (row['TIMESTAMP'], row['GPS_LATITUDE'], row['GPS_LONGITUDE'], row['SPEED'], row['EVENT_NO_TRIP'])
                    )
                    conn.commit()
      
                # Compress and upload the CSV file to GCS
                compress_and_upload_file(tempfile_path, bucket_name)
                print(f"Completed writing breadcrumb records to database and uploading the file to the bucket.)")
                os.remove(tempfile_path)
                first_operation_date = None

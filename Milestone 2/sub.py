import json
import os
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from datetime import datetime,timedelta
import csv
import tempfile
import pandas as pd
import psycopg2

#Project ID and Subscription ID
project_id = "trimet-421005"
subscription_id = "busdata-sub"

ct = 0
#database connection
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="helloworld",
    host="localhost",
    port="5432"
)

# Initialize received_messages counter
received_messages = 0

#opening the connection
cursor = conn.cursor()

# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

#temp file header
header = ['VEHICLE_ID', 'EVENT_NO_TRIP', 'EVENT_NO_STOP', 'ACT_TIME', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'METERS','OPD_DATE','GPS_SATELLITES','GPS_HDOP']

processed_combinations = set()
first_operation_date = None


today = datetime.now().strftime("%Y-%m-%d")
tempfile_path = f"breadcrumb_records_{today}.csv"

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global received_messages
    received_messages += 1
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

    #data transformation
    with open(tempfile_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        if os.stat(tempfile_path).st_size == 0:
            writer.writerow(header)
        writer.writerow([vehicle_id, trip_number, stop_number, actual_time, latitude, longitude, meters,operation_date,satellites,hdop])

    
    print(f"Received {received_messages} messages.")
    message.ack()

print(f"Listening for messages on {subscription_path}..\n")

if __name__ == "__main__":
   
   #run the subscriber continuously 
   while True:
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(project_id, subscription_id)  
        with subscriber:
            streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
            try:
                streaming_pull_future.result(timeout=300)
            except TimeoutError:
                print(f"Writing breadcrumb records to database.")
                df = pd.read_csv(tempfile_path)
                df['OPD_DATE'] = pd.to_datetime(df['OPD_DATE'], format='%d%b%Y:%H:%M:%S')

                for index, row in df.iterrows():
                     trip_id = row['EVENT_NO_TRIP']
                     vehicle_id = row['VEHICLE_ID']
    
                # Insert the row into the database
                     cursor.execute(
                     "INSERT INTO trip (trip_id, vehicle_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (trip_id, vehicle_id)
                     )
                     conn.commit()   
                print("Trip records dumped")             
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
                df_concatenated = df_concatenated.drop(['dMETERS', 'dTIMESTAMP','GPS_SATELLITES','GPS_HDOP','OPD_DATE','ACT_TIME','VEHICLE_ID','EVENT_NO_STOP','METERS'], axis=1)

                # Use the second speed value for the first speed value
                df_concatenated['SPEED'] = df_concatenated['SPEED'].bfill()

                # Ensure GPS_LATITUDE and GPS_LONGITUDE are float and handle any missing values
                df_concatenated['GPS_LATITUDE'] = pd.to_numeric(df_concatenated['GPS_LATITUDE'], errors='coerce').fillna(0.0)
                df_concatenated['GPS_LONGITUDE'] = pd.to_numeric(df_concatenated['GPS_LONGITUDE'], errors='coerce').fillna(0.0)

                # Rearrange the columns to match the DB schema
                df_concatenated = df_concatenated[['TIMESTAMP', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED', 'EVENT_NO_TRIP']]

                # Ensure TIMESTAMP is in the correct format
                df_concatenated['TIMESTAMP'] = pd.to_datetime(df_concatenated['TIMESTAMP'], format='%Y-%m-%d %H:%M:%S')

                # Save to CSV
                df_concatenated.to_csv("processed_breadcrumb_records.csv", index=False)

                # Insert into breadcrumb table
                with open("processed_breadcrumb_records.csv", 'r') as f:
                    next(f)  # Skip the header
                    cursor.copy_from(f, 'breadcrumb', sep=',')
                conn.commit()
                print("Breadcrumb records dumped")
                #Delete the temporary file
                #os.remove(tempfile_path)
            except Exception as e:
                print(f"Error: {e}")
                streaming_pull_future.cancel()
                streaming_pull_future.result()


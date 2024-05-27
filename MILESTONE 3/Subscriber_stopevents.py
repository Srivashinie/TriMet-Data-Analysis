import json
import os
import csv
import pandas as pd
import psycopg2
from google.cloud import pubsub_v1, storage
from datetime import datetime, timedelta
import gzip

# TODO: Replace with your project details
project_id = "data-engineering-420102"
subscription_id = "stopevents-sub"
subscription_path = pubsub_v1.SubscriberClient().subscription_path(project_id, subscription_id)
bucket_name = "busdata-archive" 


first_service_key = None
last_pattern_distance = {}
today = datetime.now().strftime("%Y-%m-%d")
#file_name = f"received_event_{today}.json"
#Temporary file path and header for CSV
tempfile_path = 'tempassertion_data1.csv'
header = ['vehicle_number', 'route_number', 'direction', 'service_key', 'trip_id']

#PostgreSQL connection details
db_config = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'sridhamo',
    'host': 'localhost',
    'port': '5432'
}

# Mapping of service_key and direction to enum types
service_key_map = {
    'M': 'Weekday',
    'S': 'Saturday',
    'U': 'Sunday',
    'W': 'Weekday'  
}

direction_map = {
    '0': 'Out',
    '1': 'Back'
}

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    json_message = json.loads(message.data.decode('utf-8'))
    
    vehicle_number = json_message.get('vehicle_number', None)
    leave_time = json_message.get('leave_time', None)
    train = json_message.get('train', None)
    route_number = json_message.get('route_number', None)
    direction = json_message.get('direction', None)
    service_key = json_message.get('service_key', None)
    trip_number = json_message.get('trip_number', None)
    stop_time = json_message.get('stop_time', None)
    arrive_time = json_message.get('arrive_time', None)
    dwell = json_message.get('dwell', None)
    location_id = json_message.get('location_id', None)
    door = json_message.get('door', None)
    lift = json_message.get('lift', None)
    ons = json_message.get('ons', None)
    offs = json_message.get('offs', None)
    estimated_load = json_message.get('estimated_load', None)
    maximum_speed = json_message.get('maximum_speed', None)
    train_mileage = json_message.get('train_mileage', None)
    pattern_distance = json_message.get('pattern_distance', None)
    location_distance = json_message.get('location_distance', None)
    x_coordinate = json_message.get('x_coordinate', None)
    y_coordinate = json_message.get('y_coordinate', None)
    data_source = json_message.get('data_source', None)
    schedule_status = json_message.get('schedule_status', None)
    vehicle_id = json_message.get('vehicle_id', None)
    trip_id = json_message.get('trip_id', None)

    # Assertion1: The direction cannot be null
    if direction is None:
        print("Error: Missing direction in message")
        return
    
    # Assertion2: The trip number cannot be null
    if trip_number is None:
        print("Error: Missing trip number in message")
        return
    
    # Assertion3: Service key cannot be null
    if service_key is None:
        print("Error: Missing service key in message")
        return
    
    # Assertion3: Route number cannot be null
    if route_number is None:
        print("Error: Missing route number in message")
        return
    
    # Assertion4: The direction can be 0 or 1
    if direction not in ['0', '1']:
        print("Error: Invalid direction value")
        return
    
    # Assertion5: The service key can be M, S, U, or W
    if service_key not in ['M', 'S', 'U', 'W']:
        print("Error: Invalid service key value")
        return

    # Store the first service key
    global first_service_key
    if first_service_key is None:
        first_service_key = service_key
    
    # Assertion6: For any particular day, all of the trips will have the same service key
    if service_key != first_service_key:
        print("Error: Service key does not match the first service key")
        return
    
    
    # Assertion7: Arrival time must be less than or equal to the leave time
    if arrive_time is not None and leave_time is not None and arrive_time > leave_time:
        print("Error: Arrival time is greater than leave time")
        return
    
    # Assertion8: Every x coordinate will have a y coordinate
    if (x_coordinate is None and y_coordinate is not None) or (x_coordinate is not None and y_coordinate is None):
        print("Error: Inconsistent coordinates")
        return

    # Data transformation
    
    with open(tempfile_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        #Write the header if file is empty
        if os.stat(tempfile_path).st_size == 0:
            writer.writerow(header)
        writer.writerow([vehicle_number, route_number, direction, service_key, trip_id])
    
    message.ack()

def process_csv_file():
    df = pd.read_csv(tempfile_path)
    
    # Fill missing values with the next values within each group
    df = df.groupby('trip_id', group_keys=False).apply(lambda group: group.bfill()).reset_index(drop=True)
    
    df.to_csv(tempfile_path, index=False)
    
    # Update data in PostgreSQL table
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    val='Out'
    for index, row in df.iterrows():
        if str(row['direction']) == '1':
            val = 'Back'
        else:
            val = 'Out'
        cursor.execute(
            """
            UPDATE Trip
            SET service_key = %s,
                route_id = %s,
                direction = %s
            WHERE trip_id = %s AND vehicle_id = %s
            """,
            (
                service_key_map[row['service_key']], 
                row['route_number'],
                val,
                row['trip_id'], 
                row['vehicle_number']
            )
        )
    
    conn.commit()
    cursor.close()
    conn.close()

def archive_file_to_bucket():
    #Compress the file
    compressed_file_name = f"{tempfile_path}.gz"
    with open(tempfile_path, 'rb') as f_in:
        with gzip.open(compressed_file_name, 'wb') as f_out:
            f_out.writelines(f_in)
    
    #Upload the compressed file to the storage bucket
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"archived_data/{compressed_file_name}")
    blob.upload_from_filename(compressed_file_name)
    print(f"Compressed file {compressed_file_name} uploaded to {bucket_name}.")

    os.remove(compressed_file_name)

print(f"Listening for messages on {subscription_path}..\n")

subscriber = pubsub_v1.SubscriberClient()

while True:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    
    #Keep the subscriber listening indefinitely
    with subscriber:
        try:
            streaming_pull_future.result(timeout=500) 
        except Exception as e:
            print(f"Listening error: {e}")
            streaming_pull_future.cancel()
            streaming_pull_future.result()
 
    first_service_key = None
    #Process the file only if there is data
    if os.stat(tempfile_path).st_size > 0:
       print("Processing the data") 
       process_csv_file()
       archive_file_to_bucket()
       os.remove(tempfile_path) 

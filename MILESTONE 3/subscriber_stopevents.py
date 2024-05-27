import json
import os
import gzip
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1, storage
from datetime import datetime
from psycopg2.extras import execute_batch
import csv
import psycopg2

project_id = "data-engineering-420102"
subscription_id = "stopevents-sub"
subscription_path = pubsub_v1.SubscriberClient().subscription_path(project_id, subscription_id)
bucket_name = "busdata-archive"

# Database connection details
db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'sridhamo',
    'host': 'localhost',
    'port': '5432'
}

# Initialize received_messages counter
received_messages = 0
first_operation_date = None
today = datetime.now().strftime("%Y-%m-%d")
file_name = f"received_eventsassert_copytable_{today}.json"
tempfile_path = f"stop_data_{today}.csv"
header = ['vehicle_number', 'route_number', 'direction', 'service_key', 'trip_id']

def create_stop_event_table(cur):
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS StopEvent (
                vehicle_number INT,
                route_number INT,
                direction VARCHAR(10),
                service_key VARCHAR(10),
                trip_id INT
            );
        """)
        conn.commit()
        print("Table StopEvent created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")

def copy_csv_to_db(file_path, table_name, cur):
    try:
        if os.stat(file_path).st_size == 0:
            print(f"{file_path} is empty. Skipping database insertion.")
            return

        print("Inserting data into table...")
        with open(file_path, 'r') as f:
            next(f)  # Skip the header row
            rows = [row for row in csv.reader(f)]

        insert_statement = f"""
            INSERT INTO {table_name} (
                vehicle_number, route_number, direction, service_key, trip_id
            ) VALUES (
                %s, %s, %s, %s, %s
            )
        """
        execute_batch(cur, insert_statement, rows)
        conn.commit()
        print(f"Data inserted into {table_name} successfully.")
    except Exception as e:
        print(f"Error copying data to database: {e}")

def compress_file(file_path):
    compressed_file_path = file_path + ".gz"
    with open(file_path, 'rb') as f_in:
        with gzip.open(compressed_file_path, 'wb') as f_out:
            f_out.writelines(f_in)
    return compressed_file_path

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global received_messages
    received_messages += 1
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
    
    # Map the service key value
    service_key_map = {'W': 'Weekday', 'U': 'Sunday', 'S': 'Saturday'}
    if service_key in service_key_map:
        service_key = service_key_map[service_key]

    # Map the direction value
    direction_map = {'0': 'Out', '1': 'Back'}
    if direction in direction_map:
        direction = direction_map[direction]
        
    with open(tempfile_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        if os.stat(tempfile_path).st_size == 0:
            writer.writerow(header)
        writer.writerow([vehicle_number, route_number, direction, service_key, trip_id])

    with open(file_name, 'a') as file:
        file.write(json.dumps(json_message) + '\n')

    print(f"Received {received_messages} messages.")
    message.ack()

print(f"Listening for messages on {subscription_path}..\n")

conn = psycopg2.connect(**db_params)
cur = conn.cursor()

if __name__ == "__main__":
    try:
        create_stop_event_table(cur)
        while True:
            subscriber = pubsub_v1.SubscriberClient()
            subscription_path = subscriber.subscription_path(project_id, subscription_id)
            with subscriber:
                streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
                try:
                    streaming_pull_future.result(timeout=10)
                except TimeoutError:
                    print(f"Dumping csv records to database.")
                    copy_csv_to_db(tempfile_path, 'stopevent', cur)
                    print(f"Completed Dumping csv records to database.")

                    if os.path.exists(tempfile_path) and os.stat(tempfile_path).st_size > 0:
                        print(f"Compressing file {tempfile_path}.")
                        compressed_file_path = compress_file(tempfile_path)
                        destination_blob_name = os.path.basename(compressed_file_path)
                        upload_to_gcs(bucket_name, compressed_file_path, destination_blob_name)
                        os.remove(tempfile_path)
                        os.remove(compressed_file_path)
                    else:
                        print(f"{tempfile_path} is empty. Skipping compression and upload.")
                
                except Exception as e:
                    print(f"Error: {e}")
                    streaming_pull_future.cancel()
                    streaming_pull_future.result()

    finally:
        cur.close()
        conn.close()

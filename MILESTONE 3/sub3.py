import json
import os
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from datetime import datetime, timedelta
from psycopg2.extras import execute_batch
import csv
import tempfile
import pandas as pd
import psycopg2

project_id = "trimet-421005" 
subscription_id = "busdata2-sub"  
subscription_path = pubsub_v1.SubscriberClient().subscription_path(project_id, subscription_id)

# Database connection details
db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'helloworld',
    'host': 'localhost',
    'port': '5432'
}

# Initialize received_messages counter
received_messages = 0

today = datetime.now().strftime("%Y-%m-%d")
file_name = f"received_eventsassert_copytable_{today}.json"
tempfile_path = 'tempassertion_data.csv'
header = ['vehicle_number', 'leave_time', 'train', 'route_number', 'direction', 'service_key', 'trip_number',
          'stop_time', 'arrive_time', 'dwell', 'location_id', 'door', 'lift', 'ons', 'offs', 'estimated_load',
          'maximum_speed', 'train_mileage', 'pattern_distance', 'location_distance', 'x_coordinate', 'y_coordinate',
          'data_source', 'schedule_status', 'trip_id']


def create_stop_event_table(cur):
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS StopEvent (
                vehicle_number INT,
                leave_time INT,
                train INT,
                route_number INT,
                direction VARCHAR(10),
                service_key VARCHAR(10),
                trip_number INT,
                stop_time INT,
                arrive_time INT,
                dwell INT,
                location_id INT,
                door INT,
                lift INT,
                ons INT,
                offs INT,
                estimated_load INT,
                maximum_speed INT,
                train_mileage FLOAT,
                pattern_distance FLOAT,
                location_distance FLOAT,
                x_coordinate FLOAT,
                y_coordinate FLOAT,
                data_source INT,
                schedule_status INT,
                vehicle_id INT,
                trip_id INT
            );
        """)
        conn.commit()  # Commit after creating the table
        print("Table StopEvent created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")


def copy_csv_to_db(file_path, table_name, cur):
    try:
        print("Inserting data into table...")
        with open(file_path, 'r') as f:
            next(f)  # Skip the header row
            rows = [row for row in csv.reader(f)]
            
        # Construct the SQL INSERT statement
        insert_statement = f"""
            INSERT INTO {table_name} (
                vehicle_number, leave_time, train, route_number, direction, service_key, trip_number,
                stop_time, arrive_time, dwell, location_id, door, lift, ons, offs, estimated_load,
                maximum_speed, train_mileage, pattern_distance, location_distance, x_coordinate,
                y_coordinate, data_source, schedule_status, trip_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
        """
        
        # Execute the INSERT statement using execute_batch
        execute_batch(cur, insert_statement, rows)
        
        conn.commit()  # Commit after inserting all rows
        print(f"Data inserted into {table_name} successfully.")
    except Exception as e:
        print(f"Error copying data to database: {e}")


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global received_messages
    received_messages += 1
    json_message = json.loads(message.data.decode('utf-8'))

    # Extract data from JSON message
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
        #message.ack()
        return
    
    # Assertion2: The trip number cannot be null
    if trip_number is None:
        print("Error: Missing trip number in message")
        #message.ack()
        return
    
    # Assertion3: Service key cannot be null
    if service_key is None:
        print("Error: Missing service key in message")
        #message.ack()
        return
    
    # Assertion4: The direction can be 0 or 1
    if direction not in ['0', '1']:
        print("Error: Invalid direction value")
        #message.ack()
        return
    
    # Assertion5: The service key can be M, S, U, or W
    if service_key not in ['M', 'S', 'U', 'W']:
        print("Error: Invalid service key value")
        #message.ack()
        return

    # Store the first service key
    global first_service_key
    if first_service_key is None:
        first_service_key = service_key
    
    # Assertion7: For any particular day, all of the trips will have the same service key
    if service_key != first_service_key:
        print("Error: Service key does not match the first service key")
        #message.ack()
        return
    
    
    # Assertion9: Arrival time must be less than or equal to the leave time
    if arrive_time is not None and leave_time is not None and arrive_time > leave_time:
        print("Error: Arrival time is greater than leave time")
        #message.ack()
        return
    
    # Assertion10: Every x coordinate will have a y coordinate
    if (x_coordinate is None and y_coordinate is not None) or (x_coordinate is not None and y_coordinate is None):
        print("Error: Inconsistent coordinates")
        #message.ack()
        return

    # Data transformation
    with open(tempfile_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        # Write header if the file is empty
        if os.stat(tempfile_path).st_size == 0:
            writer.writerow(header)
        writer.writerow([vehicle_number, leave_time, train, route_number, direction, service_key, trip_number,
                         stop_time, arrive_time, dwell, location_id, door, lift, ons, offs, estimated_load,
                         maximum_speed, train_mileage, pattern_distance, location_distance, x_coordinate,
                         y_coordinate, data_source, schedule_status, trip_id])

    with open(file_name, 'a') as file:
        file.write(json.dumps(json_message) + '\n')

    print(f"Received {received_messages} messages.")
    message.ack()

print(f"Listening for messages on {subscription_path}..\n")

# Opening the connection
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

if __name__ == "__main__":
    try:
        create_stop_event_table(cur)

        # Run the subscriber continuously
        while True:
            subscriber = pubsub_v1.SubscriberClient()
            subscription_path = subscriber.subscription_path(project_id, subscription_id)
            with subscriber:
                streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
                try:
                    streaming_pull_future.result(timeout=180)
                except TimeoutError:
                    print(f"Dumping csv records to database.")
                    # Copy the CSV data to the database
                    copy_csv_to_db(tempfile_path, 'stopevent', cur)
                    print(f"Completed Dumping csv records to database.")

                    #logic to update trip table
                    print(f"Completed updating stopevent records to trip.")

                    os.remove(tempfile_path)

                except Exception as e:
                    print(f"Error: {e}")
                    streaming_pull_future.cancel()
                    streaming_pull_future.result()

    finally:
        cur.close()
        conn.close()

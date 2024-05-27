import json
import os
import csv
from google.cloud import pubsub_v1
from datetime import datetime
import psycopg2

# TODO: Replace with your project details
project_id = "trimet-421005"
subscription_id = "stop-topic-sub"
subscription_path = pubsub_v1.SubscriberClient().subscription_path(project_id, subscription_id)

# Database connection details
db_params = {
       'dbname':'postgres',
       'user':'postgres',
       'password':'helloworld',
       'host':'localhost',
       'port':'5432'
}

# File to store processed combinations for uniqueness check
received_messages = 0
processed_combinations = set()
first_service_key = None
last_pattern_distance = {}
today = datetime.now().strftime("%Y-%m-%d")
file_name = f"received_eventsassert_copytable_{today}.json"
# Temporary file path and header for CSV
tempfile_path = 'tempassertion_data.csv'
header = ['vehicle_number', 'leave_time', 'train', 'route_number', 'direction', 'service_key', 'trip_number', 'stop_time', 'arrive_time', 'dwell', 'location_id', 'door', 'lift', 'ons', 'offs', 'estimated_load', 'maximum_speed', 'train_mileage', 'pattern_distance', 'location_distance', 'x_coordinate', 'y_coordinate', 'data_source', 'schedule_status','trip_id']

def copy_csv_to_db(file_path, table_name, db_params):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        with open(file_path, 'r') as f:
            next(f)  # Skip the header row
            cur.copy_from(f, table_name, sep=',', null='')
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error copying data to database: {e}")

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
    
    last_pattern_distance[trip_number] = pattern_distance
    
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

    # Map the service key value
    service_key_map = {'W': 'Weekday', 'U': 'Sunday', 'S': 'Saturday'}
    if service_key in service_key_map:
        service_key = service_key_map[service_key]

    # Map the direction value
    direction_map = {'0': 'Out', '1': 'Back'}
    if direction in direction_map:
        direction = direction_map[direction]

    # Data transformation
    with open(tempfile_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([vehicle_number, leave_time, train, route_number, direction, service_key, trip_number, stop_time, arrive_time, dwell, location_id, door, lift, ons, offs, estimated_load, maximum_speed, train_mileage, pattern_distance, location_distance, x_coordinate, y_coordinate, data_source, schedule_status, trip_id])
    
    with open(file_name, 'a') as file:
        file.write(json.dumps(json_message) + '\n')
    
    # Copy the CSV data to the PostgreSQL table
    copy_csv_to_db(tempfile_path, 'stop_events', db_params)
    print("inserted {received_messages} data to db")
    message.ack()

print(f"Listening for messages on {subscription_path}..\n")

subscriber = pubsub_v1.SubscriberClient()
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

# Keep the subscriber listening
with subscriber:
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        streaming_pull_future.result()


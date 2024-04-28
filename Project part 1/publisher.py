import json
import time
import requests
from datetime import datetime
from google.cloud import pubsub_v1
import urllib.request
import urllib.error

# TODO(developer)
project_id = "data-engineering-420102"
topic_id = "busdata"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

#url = 'https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={}'
vehicle_ids = [
    '4505', '3257', '3232', '4033', '4040', '3940', '3313', '3609', '3738', '3960', '3625', '3126',
    '4046', '4207', '3263', '4513', '3253', '3567', '3407', '4017', '3132', '4032', '3107', '3101', '4002',
    '3265', '3266', '3943', '2912', '3526', '4502', '4016', '2920', '3707', '3926', '4205', '3616', '3139',
    '3731', '3753', '3537', '3717', '3038', '3148', '3228', '2918', '3516', '3150', '3614', '3213', '3417',
    '2935', '3644', '2936', '3649', '3160', '3144', '3569', '3034', '4230', '4211', '2919', '2931', '4056',
    '4238', '3028', '3202', '3019', '2932', '3937', '3939', '3715', '4047', '3022', '3328', '4019', '3219',
    '3532', '2908', '3751', '3114', '3217', '3231', '3119', '3203', '3327', '3008', '3023', '3230', '3727',
    '3906', '4227', '3748', '2939', '3225', '3131', '3421', '3021', '3802', '3643'
]

message_count = 0

def publish_to_pubsub(message):
    """Publish a message to the configured GCP Pub/Sub topic."""
    #global message_count
    try:
        message_bytes = message.encode('utf-8')
        future = publisher.publish(topic_path, message_bytes)
        future.result()
        #message_count += 1
        #time.sleep(0.0065)
    except Exception as e:
        print(f"An error occurred while publishing: {e}")

def fetch_and_publish_data(vehicle_id):
    """Fetch JSON data for a vehicle and publish each record to GCP Pub/Sub."""
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    global message_count
    try:
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())
            # Assuming the JSON structure contains a list of records
            for record in data:
                publish_to_pubsub(json.dumps(record))
                message_count += 1
            print(f"Processed vehicle ID {vehicle_id}")
    except urllib.error.URLError as e:
        print(f"Failed to fetch data for vehicle ID {vehicle_id}: {e}")

def main():
    for vehicle_id in vehicle_ids:
        fetch_and_publish_data(vehicle_id)

if __name__ == "__main__":
    main()

print(f"Published {message_count} messages to {topic_path}.")
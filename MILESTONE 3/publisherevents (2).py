import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from google.cloud import pubsub_v1

# List of vehicle IDs to query
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

# Base URL for the API endpoint
base_url = "https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num="
ct=0
# Google Cloud Pub/Sub configuration
project_id = "data-engineering-420102"  # TODO: replace with your GCP project ID
topic_id = "stopevents"  # TODO: replace with your Pub/Sub topic ID

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Function to fetch data for each vehicle ID
def fetch_data(vehicle_ids):
    data_list = []

    for vehicle_id in vehicle_ids:
        url = base_url + vehicle_id
        response = requests.get(url)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            trip_heading = soup.find('h2')
            trip_id = None
            if trip_heading:
                trip_id = trip_heading.text.split()[-1]
            
            table = soup.find('table')
            if table:
                headers = [header.text for header in table.find_all('th')]
                rows = table.find_all('tr')
                
                for row in rows[1:]:  # Skip the header row
                    columns = row.find_all('td')
                    data = {headers[i]: columns[i].text for i in range(len(headers))}
                    data['vehicle_id'] = vehicle_id
                    data['trip_id'] = trip_id
                    data_list.append(data)
            else:
                print(f"No data table found for vehicle ID {vehicle_id}.")
        else:
            print(f"Failed to fetch data for vehicle ID {vehicle_id}, status code: {response.status_code}")
    
    return data_list

# Function to publish data to Pub/Sub
def publish_to_pubsub(data):
    global ct
    try:
        message_json = json.dumps(data)
        message_bytes = message_json.encode('utf-8')
        #print(message_bytes)
        future = publisher.publish(topic_path, message_bytes)
        future.result()
        ct+=1
        #print(f"Published message for vehicle ID {data['vehicle_id']}")
    except Exception as e:
        print(f"An error occurred while publishing: {e}")

# Fetch data and publish it to Pub/Sub
def main():
    data_list = fetch_data(vehicle_ids)
    for data in data_list:
        publish_to_pubsub(data)

if __name__ == "__main__":
    main()

print(ct)

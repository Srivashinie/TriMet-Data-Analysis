import json
import os
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from datetime import datetime

# TODO(developer)
project_id = "data-engineering-420102"
subscription_id = "bussub"
# Number of seconds the subscriber should listen for messages

ct = 0
subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)
today = datetime.now().strftime("%Y-%m-%d")
file_name = f"received_messages_{today}.json"
# Open a file to write the messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    json_message = json.loads(json.dumps(message.data.decode('utf-8')))
    with open(file_name, 'a') as file:
        file.write(json_message + '\n')
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        #streaming_pull_future.result()
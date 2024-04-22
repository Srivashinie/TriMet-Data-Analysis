from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from datetime import datetime
import sys
import os

# TODO(developer)
project_id = "trimet-421005"
subscription_id = "mysub"
# Number of seconds the subscriber should listen for messages
timeout = 200.0

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received {message}.")
    message.ack()

#file_name = input("Enter the file name to write the result to: ")
current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
folder_name = "TriMetSubscribedRecords"
file_name = f"Result_{current_datetime}.json"
filepath = os.path.join(folder_name, file_name)
if not os.path.exists(folder_name):
    os.makedirs(folder_name)

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

result = None

with open(filepath, "w") as file:
    original_stdout = sys.stdout
    sys.stdout = file
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            # Wait for messages
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result()
        finally:
            file.close()
            sys.stdout = original_stdout
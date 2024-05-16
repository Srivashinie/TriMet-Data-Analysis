import requests
from bs4 import BeautifulSoup
import json
import datetime

# List of vehicle IDs
vehicle_ids = [
    '4505', '3257', '3232', '4033', '4040', '3940', '3313', '3609', '3738', '3960', '3625', '3126',
    '4046', '4207', '3263', '4513', '3253', '3567', '3407', '4017', '3132', '4032', '3107', '3101', '4002',
    '3265', '3266', '3943', '2912', '3526', '4502', '4016', '2920', '3707', '3926', '4205', '3616', '3139',
    '3731', '3753', '3537', '3717', '3038', '3148', '3228', '2918', '3516', '3150', '3614', '3213', '3417',
    '2935', '3644', '2936', '3649', '3160', '3144', '3569', '3034', '4230', '4211', '2919', '2931', '4056',
    '4238', '3028', '3202', '3019', '2932', '3937', '3939', '3715', '4047', '3022', '3328', '4019', '3219',
    '3532', '2908', '3751', '3114', '3217', '3231', '3119', '3203', '3327', '3008', '3023', '3230', '3727',
    '3906', '4227', '3748', '2939', '3225', '3131', '3421', '3021', '3802', '3643'
]  # Add your vehicle IDs here

# Initialize an empty list to store the data
data_list = []
# Output file
now = datetime.datetime.now()
date_str = now.strftime("%Y-%m-%d")
output_file = f'StopEventData/Ragnarok_StopEvent_{date_str}.json'

for vehicle_id in vehicle_ids:
    # URL of the API endpoint that returns the rendered HTML
    api_url = f'https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={vehicle_id}'

    # Send a GET request to the API endpoint
    response = requests.get(api_url)

    # Check if the request was successful
    if response.status_code == 200:
        # Get the HTML content from the response
        html_content = response.text

        # Parse the HTML content
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find all h2 elements
        h2_tags = soup.find_all('h2')

        for h2 in h2_tags:
            # Get the text of the h2 element
            h2_text = h2.text.strip()  # Strip any leading/trailing whitespace
            last_word = h2_text.split()[-1]

            # Find the next table tag after this h2 tag
            table = h2.find_next_sibling('table')

            if table:
                # Find all rows in the table
                rows = table.find_all('tr')

                # Get the header row to use as keys
                header_row = [cell.text.strip() for cell in rows[0].find_all(['th', 'td'])]

                # Iterate over each row and store the content as key-value pairs
                for row in rows[1:]:
                    cells = row.find_all(['th', 'td'])
                    row_data = {}
                    #row_data['vehicle_number'] = vehicle_id
                    row_data['EVENT_NO_TRIP'] = last_word  # Add the event_no to the row data
                    for i, cell in enumerate(cells):
                        row_data[header_row[i]] = cell.text.strip()
                    data_list.append(row_data)

    else:
        print(f'Failed to fetch HTML content from the API for vehicle ID: {vehicle_id}')

# Write the data to a single JSON file
with open(output_file, 'w') as json_file:
    json.dump(data_list, json_file, indent=4)

print('Data dumped to json file')

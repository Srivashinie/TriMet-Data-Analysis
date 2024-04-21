import os
import requests
from datetime import datetime
def fetch_data(vehicle_id):
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    response = requests.get(url)
    return response.content.decode("utf-8")
if __name__ == "__main__":
    vehicle_ids = [4505,3257,3232,4033,4040,3940,3313,3609,3738,3960,3625,3126,4046,4207,3263,4513,3253,3567,3407,4017,3132,4032,3107,3101,4002,3265,3266,3943,2912,3526,4502,4016,2920,3707,3926,4205,3616,3139,3731,3753,3537,3717,3038,3148,3228,2918,3516,3150,3614,3213,3417,2935,3644,2936,3649,3160,3144,3569,3034,4230,4211,2919,2931,4056,4238,3028,3202,3019,2932,3937,3939,3715,4047,3022,3328,4019,3219,3532,2908,3751,3114,3217,3231,3119,3203,3327,3008,3023,3230,3727,3906,4227,3748,2939,3225,3131,3421,3021,3802,3643]  # Ragnarok Vehicle ids
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    folder_name = "TriMet"
    filename = f"Ragnarok_{current_datetime}.txt"
    filepath = os.path.join(folder_name, filename)
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    with open(filepath, "a") as file:
        for vehicle_id in vehicle_ids:
            response = fetch_data(vehicle_id)
            file.write(f"Response for vehicle ID {vehicle_id}:\n{response}\n\n")
    print(f"All responses have been appended to the file '{filepath}'.")

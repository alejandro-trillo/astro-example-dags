from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests

#########################################################
#####               VARIABLES          ##################
#########################################################
this_dag = 'astronomer_distance_and_directions_dag'
dag_description = 'A DAG to get driving distance and directions between a patient address and the appointment location'

origin_lat = 35.90551         #  8501 Brier Creek Pkwy, Raleigh, NC 27617   
origin_lng = -78.7852         #  8501 Brier Creek Pkwy, Raleigh, NC 27617   
destination_lat = 36.0040     #  Duke University Hospital in Durham, NC  
destination_lng = -78.9368    #  Duke University Hospital in Durham, NC 

origin_addr = '8501 Brier Creek Pkwy, Raleigh, NC 27617'
destination_addr = '2301 Erwin Rd, Durham, NC 27710'

google_map_api_key = 'AIzaSyBBiaxfQd_yNliOt2XD9ONouAWey9-_nlI'
azure_subscription_key = '6sY9HgfKD0KyTndmJGNSqctNh7mu9A6WNOn71AKzzdnf4bKpF648JQQJ99BEACYeBjF6AQLKAAAgAZMP3Jiw'

google_maps_url = 'https://www.google.com/maps/dir/?api=1&origin=<<<ORIGIN_ADDR>>>&destination=<<<DESTINATION_ADDR>>>'

#########################################################
#####               FUNCTIONS          ##################
#########################################################

def use_google(**kwargs):
    origin = kwargs['origin']
    destination = kwargs['destination']
    api_key = kwargs['api_key']
    origin_lat, origin_lng = get_latitude_and_longitude_using_address(address=origin, api_key=api_key)
    destination_lat, destination_lng = get_latitude_and_longitude_using_address(address=destination, api_key=api_key)
    distance, driving_directions = get_distance_directions_using_address(origin=origin, destination=destination, api_key=api_key)


def use_azure(**kwargs):
    origin = kwargs['origin']
    destination = kwargs['destination']
    api_key = kwargs['api_key']
    get_distance_directions_with_azure(origin=origin, destination=destination, api_key=api_key)


# Function that uses Google Maps API to get lat and lng
def get_latitude_and_longitude_using_address(**kwargs):
    address = kwargs['address']
    api_key = kwargs['api_key']
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={api_key}"

    response = requests.get(url)
    data = response.json()

    if data['status'] == 'OK':
        location = data['results'][0]['geometry']['location']
        lat = location['lat']
        lng = location['lng']
        print(f"Latitude: {lat}")
        print(f"Longitude: {lng}")
        return lat, lng
    else:
        print("Error:", data['status'])
        return None, None


# Function that uses Google Maps API to get distance and directions using addresses
def get_distance_directions_using_address(**kwargs):
    origin = kwargs['origin']
    destination = kwargs['destination']
    api_key = kwargs['api_key']
    url = f"https://maps.googleapis.com/maps/api/directions/json?origin={origin}&destination={destination}&key={api_key}"

    response = requests.get(url)
    data = response.json()

    if data['status'] == 'OK':
        distance = data['routes'][0]['legs'][0]['distance']['text']
        duration = data['routes'][0]['legs'][0]['duration']['text']
        directions = data['routes'][0]['legs'][0]['steps']

        directions_list = []
        for step in directions:
            directions_list.append(step['html_instructions'])

        print(f"Distance: {distance}")
        print(f"Duration: {duration}")
        print("Directions:")
        for direction in directions_list:
            print(direction)

        driving_directions = ' '.join(directions_list)
        return distance, driving_directions
    else:
        print("Error:", data['status'])
        return None, None

# Function that uses Google Maps API to get distance and directions using geocodes
def get_distance_directions_using_geocodes(**kwargs):
    origin_lat = kwargs['origin_lat']
    origin_lng = kwargs['origin_lng']
    destination_lat = kwargs['destination_lat']
    destination_lng = kwargs['destination_lng']
    api_key = kwargs['api_key']
    url = f"https://maps.googleapis.com/maps/api/directions/json?origin={origin_lat},{origin_lng}&destination={destination_lat},{destination_lng}&key={api_key}"

    response = requests.get(url)
    data = response.json()

    if data['status'] == 'OK':
        distance = data['routes'][0]['legs'][0]['distance']['text']
        duration = data['routes'][0]['legs'][0]['duration']['text']
        directions = data['routes'][0]['legs'][0]['steps']

        directions_list = []
        for step in directions:
            directions_list.append(step['html_instructions'])

        print(f"Distance: {distance}")
        print(f"Duration: {duration}")
        print("Directions:")
        for direction in directions_list:
            print(direction)
    else:
        print("Error:", data['status'])


def geocode_address_with_azure(address, azure_key):
    url = f"https://atlas.microsoft.com/search/address/json?subscription-key={azure_key}&api-version=1.0&query={address}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    coordinates = data['results'][0]['position']
    return coordinates['lat'], coordinates['lon']


def get_directions_using_azure(origin, destination, azure_key):
    origin_lat, origin_lon = geocode_address_with_azure(origin, azure_key)
    destination_lat, destination_lon = geocode_address_with_azure(destination, azure_key)

    url = f"https://atlas.microsoft.com/route/directions/json?subscription-key={azure_key}&api-version=1.0&query={origin_lat},{origin_lon}:{destination_lat},{destination_lon}&travelMode=car"
    response = requests.get(url)
    response.raise_for_status()
    directions = response.json()
    return directions


def get_distance_directions_with_azure(**kwargs):
    origin = kwargs.get('origin')
    destination = kwargs.get('destination')
    api_key = kwargs.get('api_key')
    directions = get_directions_using_azure(origin, destination, api_key)
    print(directions)


#########################################################
#####          DAG DEFINITION          ##################
#########################################################

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    this_dag,
    default_args=default_args,
    description=dag_description,
    schedule_interval=None
)

# Define the tasks
use_google_task = PythonOperator(
    task_id='use_google_task',
    python_callable=use_google,
    op_kwargs={'origin': origin_addr, 'destination': destination_addr, 'api_key': google_map_api_key},
    dag=dag
)

use_azure_task = PythonOperator(
    task_id='use_azure_task',
    python_callable=use_azure,
    op_kwargs={'origin': origin_addr, 'destination': destination_addr, 'api_key': azure_subscription_key},
    dag=dag
)

# Set task dependencies
use_google_task >> use_azure_task

from airflow.decorators import dag, task
from pendulum import datetime
import requests
import json

# Define the basic parameters of the DAG
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "TrainData", "retries": 3},
    tags=["example"],
)
def train_data_pipeline():
    
    @task
    def get_train_data() -> dict:
        """
        Fetches minimal train data from the API.
        """
        app_id = '332e4ce2'  # Replace with your actual app_id
        app_key = 'e7bc2aacb8164bce1e4dd59088b9875b'  # Replace with your actual app_key
        # Fetch data for a specific train to minimize the data retrieved
        train_uid = 'C83943'
        api_date = '2021-12-08'
        api_endpoint = f'https://transportapi.com/v3/uk/train/service/train_uid:{train_uid}/{api_date}/timetable.json?app_id={app_id}&app_key={app_key}'
        
        response = requests.get(api_endpoint)
        response.raise_for_status()
        return response.json()

    @task
    def process_train_data(train_data: dict) -> None:
        """
        Processes the train data and prints relevant information.
        """
        departures = train_data.get('departures', {}).get('all', [])
        if not departures:
            print("No departures found in the retrieved data.")
            return

        # Print minimal information
        for departure in departures[:1]:  # Process only the first departure
            print(f"Train from {departure['origin_name']} to {departure['destination_name']} "
                  f"aimed departure: {departure['aimed_departure_time']} on platform {departure['platform']}")

    # Define the task dependencies
    process_train_data(get_train_data())

# Instantiate the DAG
train_data_pipeline()

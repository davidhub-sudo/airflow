"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API and prints each astronaut's name and flying craft.

There are two tasks, one to get the data from the API and save the results,
and another to print the results. Both tasks are written in Python using
Airflow's TaskFlow API, which allows you to easily turn Python functions into
Airflow tasks, and automatically infer dependencies and pass data.

The second task uses dynamic task mapping to create a copy of the task for
each Astronaut in the list retrieved from the API. This list will change
depending on how many Astronauts are in space, and the DAG will adjust
accordingly each time it runs.

For more explanation and getting started instructions, see our Write your
first DAG tutorial: https://docs.astronomer.io/learn/get-started-with-airflow

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""
from airflow import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime
import requests
API = "https://www.boredapi.com/api/activity"


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
)

def find_activity():
    @task
    def get_activity():     # El nombre get_activity es el ID de la tarea dentro del DAG
        r = requests.get(API, timeout=10)
        return r.json()

    @task
    def write_activity_to_file(response):
        filepath = Variable.get("activity_file")
        with open(filepath, "a") as f:
            f.write(f"Today you will: {response['activity']}\r\n")
        return filepath

    @task
    def read_activity_from_file(filepath):
        with open(filepath, "r") as f:
            print(f.read())

    response = get_activity() 
    filepath = write_activity_to_file(response) 
    read_activity_from_file(filepath)

# Instantiate the DAG
find_activity()





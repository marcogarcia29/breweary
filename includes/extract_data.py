import requests
import json 
import logging


def get_data():
    url="https://api.openbrewerydb.org/breweries"

    response = requests.get(url)

    try:
        with open('/opt/airflow/bronze/data.json', 'w+') as f:
            json.dump(response.json(), f)
    except:
        logging.exception("message")

get_data()
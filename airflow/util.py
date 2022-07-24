import json


def get_secret_data() -> dict:
    with open("secret/airflow.json") as f:
        data = json.load(f)
    return data

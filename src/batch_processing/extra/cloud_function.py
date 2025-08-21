"""
Requirements for this file:

functions-framework==3.*
google-cloud-compute==1.23.0
google-cloud-storage==2.19.0
"""

from __future__ import annotations

import datetime
import json
import requests
import os
import functions_framework

from google.cloud import compute_v1, storage
from google.cloud.compute_v1.services.zone_operations import pagers


SLACK_URL = os.getenv("SLACK_URL")


def list_zone_operations(
    project_id: str, zone: str, filter: str = ""
) -> pagers.ListPager:
    """
    List all recent operations the happened in given zone in a project. Optionally filter those
    operations by providing a filter. More about using the filter can be found here:
    https://cloud.google.com/compute/docs/reference/rest/v1/zoneOperations/list
    Args:
        project_id: project ID or project number of the Cloud project you want to use.
        zone: name of the zone you want to use. For example: "us-west3-b"
        filter: filter string to be used for this listing operation.
    Returns:
        List of preemption operations in given zone.
    """
    operation_client = compute_v1.ZoneOperationsClient()
    request = compute_v1.ListZoneOperationsRequest()
    request.project = project_id
    request.zone = zone
    request.filter = filter

    return operation_client.list(request)


def preemption_history(
    project_id: str, zone: str, instance_name: str = None
) -> list[tuple[str, datetime.datetime]]:
    """
    Get a list of preemption operations from given zone in a project. Optionally limit
    the results to instance name.
    Args:
        project_id: project ID or project number of the Cloud project you want to use.
        zone: name of the zone you want to use. For example: "us-west3-b"
        instance_name: name of the virtual machine to look for.
    Returns:
        List of preemption operations in given zone.
    """
    filter = 'operationType="compute.instances.preempted"'
    history = []

    zone_operations = list_zone_operations(project_id, zone, filter)
    for operation in zone_operations:
        d = {
            "id": operation.id,
            "name": operation.name,
            "operation_type": operation.operation_type,
            "status": operation.status.name,
            "status_message": operation.status_message,
            "target_id": operation.target_id,
            "target_link": operation.target_link,
            "self_link": operation.self_link,
            "instance_name": operation.target_link.split("/")[-1],
            "start_time": operation.start_time,
            "insert_time": operation.insert_time,
            "end_time": operation.end_time,
        }

        history.append(d)

    return history


def format_date(date):
    parsed_date = datetime.datetime.fromisoformat(date)
    formatted_date = parsed_date.strftime("%B %d, %Y at %I:%M %p %Z")
    return formatted_date


def send_slack_message(url, text):
    payload = {"text": text}
    response = requests.post(url, json=payload, headers={'Content-type': 'application/json'})

    # check if the request was successful
    response.raise_for_status()


def update_records(bucket_name, file_path, data):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    existing_data = blob.download_as_text()
    existing_data = json.loads(existing_data)

    for elem in data:
        if elem not in existing_data:
            existing_data.append(elem)

            machine_name = elem.get("instance_name")
            date = format_date(elem.get("insert_time"))
            message = f"Machine {machine_name} is was preempted on {date}."
            send_slack_message(SLACK_URL, message)


    updated_data = json.dumps(existing_data, indent=4)
    blob.upload_from_string(updated_data, content_type="application/json")


@functions_framework.http
def main(request):
    data = preemption_history("spherical-berm-323321", "us-central1-c")
    update_records("gcp-slurm", "preemption_records.json", data)

    return "Update is successfully completed!"

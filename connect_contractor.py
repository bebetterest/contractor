import argparse
import requests
import json

def register_worker(url, worker_id):
    response = requests.post(f"{url}/worker_register", json={"worker_id": worker_id})
    return response.status_code, response.json()

def assign_task(url, worker_id):
    response = requests.post(f"{url}/assign_task", params={"worker_id": worker_id})
    return response.status_code, response.json()

def submit_task(url, worker_id):
    response = requests.post(f"{url}/submit_task", params={"worker_id": worker_id})
    return response.status_code, response.json()

def add_task(url, task_content, batch_size):
    if isinstance(task_content, str):
        task_content = [
            int(_)
            for _ in task_content.strip().split(" ")
        ]
    for i in range(0, len(task_content), batch_size):
        response = requests.post(
            f"{url}/add_task",
            json={"task_content": task_content[i:min(i + batch_size, len(task_content))]}
        )
    return response.status_code, response.json()

def finish_all(url):
    response = requests.post(f"{url}/finish_all")
    return response.status_code, response.json()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Interact with the contractor service")
    parser.add_argument("--url", required=True, help="Contractor service URL")
    parser.add_argument("--worker_id", help="Unique identifier for the worker")
    parser.add_argument("--type", required=True, choices=["register", "assign", "submit", "add_task", "finish_all"], help="Type of action to perform")
    parser.add_argument("--task_content", type=str, help="Content of the new task for add_task")
    parser.add_argument("--batch_size", type=int, help="Batch size for add_task")
    parser.add_argument("--output", default="contractor_res_json.log", help="Path to output JSON file")
    args = parser.parse_args()

    if args.type == "register":
        status_code, response = register_worker(args.url, args.worker_id)
    elif args.type == "assign":
        status_code, response = assign_task(args.url, args.worker_id)
    elif args.type == "submit":
        status_code, response = submit_task(args.url, args.worker_id)
    elif args.type == "add_task":
        if args.task_content is None:
            raise ValueError("task_content is required for add_task")
        if args.batch_size is None:
            raise ValueError("batch_size is required for add_task")
        status_code, response = add_task(
            args.url, args.task_content, args.batch_size
        )
    elif args.type == "finish_all":
        status_code, response = finish_all(args.url)

    print(status_code, end="")

    with open(args.output, "w") as log_file:
        log_file.write(json.dumps(response, indent=2))

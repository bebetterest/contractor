import requests
import json
import multiprocessing
import time
import random

BASE_URL = 'http://127.0.0.1:7290'

def test_worker_register(worker_id):
    url = f"{BASE_URL}/worker_register"
    data = {'worker_id': worker_id}
    response = requests.post(url, json=data)
    print(f"Worker Register Response for {worker_id}: {response.json()}")
    return response

def test_assign_task(worker_id):
    url = f"{BASE_URL}/assign_task"
    params = {'worker_id': worker_id}
    response = requests.post(url, params=params)
    print(f"Assign Task Response for {worker_id}: {response.json()}")
    return response

def test_submit_task(worker_id):
    url = f"{BASE_URL}/submit_task"
    params = {'worker_id': worker_id}
    response = requests.post(url, params=params)
    print(f"Submit Task Response for {worker_id}: {response.json()}")
    return response

def worker_process(worker_id, task_count):
    test_worker_register(worker_id)
    for _ in range(task_count):
        time.sleep(random.uniform(0.5, 1.5))
        assign_response = test_assign_task(worker_id)
        if assign_response.status_code == 200 and 'task' in assign_response.json():
            time.sleep(random.uniform(1.0, 3.0))
            test_submit_task(worker_id)

if __name__ == "__main__":
    worker_ids = [f"worker_{i}" for i in range(1, 6)]
    processes = []

    for worker_id in worker_ids:
        task_count = random.randint(1, 5)
        p = multiprocessing.Process(target=worker_process, args=(worker_id, task_count))
        processes.append(p)
        time.sleep(random.uniform(0.1, 0.5))
        p.start()

    for p in processes:
        p.join()

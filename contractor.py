import math
import time
import logging
import threading

import wandb
import pandas as pd
from flask import Flask, request, jsonify


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("contractor.log")
    ]
)
wandb.init(project="task-contractor", name=f"test_{time.strftime('%Y%m%d_%H%M%S')}")

app = Flask(__name__)
task_status: dict = {}
worker_status: dict = {}
lock = threading.Lock()


def init_tasks(
        task_infos: dict = {
            "num": 8139,
        },
        chunk_size: int = 16,
):
    logging.info("Initializing tasks")
    global worker_status
    worker_status = {}
    global task_status
    task_status = {}

    micro_task_num = math.ceil(task_infos["num"] / chunk_size)
    for idx in range(micro_task_num):
        start = idx * chunk_size
        end = min((idx + 1) * chunk_size, task_infos["num"])
        task_status[idx] = {
            "idx": idx,
            "content": list(range(start, end)),
            "done_flag": False,
            "assign_num": 0,
            "assigned_workers": [],
            "last_update_time": None,
            "time_cost": None,
        }
    assert sum(len(_["content"]) for _ in task_status.values()) == task_infos["num"]
    logging.info("Total %d tasks initialized", len(task_status))
    return


def rank_worker(worker_status_list):
    worker_status_list = sorted(
        worker_status_list,
        key=lambda x: (
            -x['reward'],
            -len(x['done_micro_tasks']),
            -x['last_update_time']
        )
    )
    return worker_status_list


def rank_task(task_status_list):
    task_status_list = sorted(
        task_status_list,
        key=lambda x: (
            x['done_flag'],
            x['assign_num'],
            x['last_update_time'] if x['last_update_time'] is not None else float('-inf')
        )
    )
    return task_status_list


def get_reward(
        task_idx, task_has_done_flag,
        worker_done_task_num, total_done_task_num, total_task_num
):
    if task_has_done_flag:
        return 1
    reward_decay_factor =\
        (total_task_num - total_done_task_num + worker_done_task_num) / total_task_num
    reward = 1 + round((1000 - 1) * reward_decay_factor)
    return reward


def sync_to_wandb():
    global task_status
    global worker_status
    global wandb_task_table
    global wandb_worker_table

    task_columns = ["idx", "done_flag", "assign_num", "assigned_workers", "time_cost", "last_update_time", "content"]
    wandb_task_table = wandb.Table(columns=task_columns)
    for task in task_status.values():
        wandb_task_table.add_data(
            task["idx"],
            task["done_flag"],
            task["assign_num"],
            str(task["assigned_workers"]),
            task["time_cost"],
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(task["last_update_time"])) if task["last_update_time"] else None,
            str(task["content"]),
        )

    worker_columns = ["worker_id", "reward", "assigned_micro_task", "last_update_time", "done_micro_tasks"]
    wandb_worker_table = wandb.Table(columns=worker_columns)
    for worker in worker_status.values():
        wandb_worker_table.add_data(
            worker["worker_id"],
            worker["reward"],
            worker["assigned_micro_task"],
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(worker["last_update_time"])) if worker["last_update_time"] else None,
            str(worker["done_micro_tasks"])
        )

    time_cost_list = [
        task['time_cost']
        for task in task_status.values()
        if task['time_cost'] is not None
    ]
    wandb_total_status = {
        "task_num": len(task_status),
        "done_task_num": sum([
            task['done_flag']
            for task in task_status.values()
        ]),
        "worker_num": len(worker_status),
        "assigned_task_num": sum([
            task['assign_num']>0
            for task in task_status.values()
        ]),
        "average_time_cost":\
            sum(time_cost_list) / len(time_cost_list) if len(time_cost_list) > 0 else -1,
        "over_assigned_task_num": sum([
            task['assign_num']>1
            for task in task_status.values()
        ]),
    }
    try:
        wandb.log({
            **wandb_total_status,
            "task_status": wandb_task_table,
            "worker_status": wandb_worker_table
        })
    except Exception as e:
        logging.error(f"Failed to sync to wandb: {e}")


@app.route('/worker_register', methods=['POST'])
def worker_register():
    """
    Register a new worker.

    Request data format:
    {
        "worker_id": "string"  # Required, unique identifier for the worker
    }

    Returns:
    - Success: Status code 200, message {'message': 'worker registered successfully'}
    - Failure: Status code 400, error message {'error': 'worker_id is required'} or {'error': 'worker_id already registered, please use a different worker_id'}
    """
    logging.info("Worker registration request received with data: %s", request.json)
    data = request.json
    worker_id = data.get('worker_id')
    if not worker_id:
        return jsonify({'error': 'worker_id is required'}), 400
    if worker_id in worker_status:
        return jsonify({'error': 'worker_id already registered, please use a different worker_id'}), 400

    with lock:
        worker_status[worker_id] = {
            "reward": 0,
            'last_update_time': time.time(),
            "assigned_micro_task": None,
            "done_micro_tasks": [],
            "worker_id": worker_id,
        }
        sync_to_wandb()
    return jsonify({'message': 'worker registered successfully'}), 200


@app.route('/assign_task', methods=['POST'])
def assign_task():
    """
    Assign a task to a worker.

    Request parameters:
    - worker_id: string  # Required, unique identifier for the worker

    Returns:
    - Success: Status code 200, task information {"task_id": int, 'task': list}
    - Failure: Status code 400, error message {'error': 'worker_id is required'} or {'error': 'worker_id not registered'} or {'error': 'worker already has a task assigned'}
    - No task: Status code 400, message {'message': 'no task to assign'} or {'message': 'all tasks are done'}
    """
    logging.info("Task assignment request received with args: %s", request.args)
    global task_status
    global worker_status

    worker_id = request.args.get('worker_id')
    if not worker_id:
        return jsonify({'error': 'worker_id is required'}), 400
    if worker_id not in worker_status:
        return jsonify({'error': 'worker_id not registered'}), 400
    if worker_status[worker_id]['assigned_micro_task'] is not None:
        return jsonify({'error': 'worker already has a task assigned'}), 400
    if len(task_status) == 0:
        return jsonify({'message': 'no task to assign'}), 400

    with lock:
        ranked_tasks = rank_task(list(task_status.values()))
        if ranked_tasks[0]['done_flag']:
            return jsonify({'message': 'all tasks are done'}), 400

        task = ranked_tasks[0]

        task_status[task['idx']]['assign_num'] += 1
        task_status[task['idx']]['assigned_workers'].append(worker_id)
        task_status[task['idx']]['last_update_time'] = time.time()

        worker_status[worker_id]['assigned_micro_task'] = task['idx']
        worker_status[worker_id]['last_update_time'] = time.time()
        sync_to_wandb()
    return jsonify({
        "task_id": task['idx'],
        'task': task["content"]
    }), 200


@app.route('/submit_task', methods=['POST'])
def submit_task():
    """
    Submit a completed task.

    Request parameters:
    - worker_id: string  # Required, unique identifier for the worker

    Returns:
    - Success: Status code 200, message {'message': 'task submitted successfully'}
    - Failure: Status code 400, error message {'error': 'worker_id is required'} or {'error': 'worker_id not registered'} or {'error': 'no task assigned to worker'}
    """
    logging.info("Task submission request received with args: %s", request.args)
    global task_status
    global worker_status

    worker_id = request.args.get('worker_id')
    if not worker_id:
        return jsonify({'error': 'worker_id is required'}), 400
    if worker_id not in worker_status:
        return jsonify({'error': 'worker_id not registered'}), 400
    if worker_status[worker_id]['assigned_micro_task'] is None:
        return jsonify({'error': 'no task assigned to worker'}), 400

    with lock:
        task_idx = worker_status[worker_id]['assigned_micro_task']

        task_has_done_flag = task_status[task_idx]['done_flag']
        task_status[task_idx]['done_flag'] = True
        task_status[task_idx]['assign_num'] -= 1
        # task_status[task_idx]['assigned_workers'].remove(worker_id)
        time_tmp = time.time()
        task_status[task_idx]['time_cost'] = time_tmp - task_status[task_idx]['last_update_time']
        task_status[task_idx]['last_update_time'] = time_tmp

        worker_status[worker_id]['done_micro_tasks'].append(task_idx)
        worker_status[worker_id]['assigned_micro_task'] = None
        worker_status[worker_id]['last_update_time'] = time.time()
        worker_status[worker_id]['reward'] += get_reward(
            task_idx, task_has_done_flag,
            len(worker_status[worker_id]['done_micro_tasks']),
            len([
                _ for _ in task_status.values()
                if _['done_flag']
            ]),
            len(task_status),
        )
        sync_to_wandb()
    return jsonify({'message': 'task submitted successfully'}), 200

if __name__ == '__main__':
    logging.info("Starting the Flask app")
    init_tasks()
    app.run(debug=False, host='0.0.0.0', port=7290)

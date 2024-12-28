from flask import Flask, request, jsonify
import threading
import threading
import logging
import wandb
import math
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
wandb.init(project=f"task-contractor_{time.strftime('%Y%m%d_%H%M%S')}", name="test")

app = Flask(__name__)
task_status: dict = {}
worker_status: dict = {}
lock = threading.Lock()


def init_tasks(
        task_infos: dict = {
            "num": 8192,
        },
        chunk_size: int = 96,
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


def sync_to_wandb():
    global task_status
    global worker_status
    wandb_task_table = rank_task(list(task_status.values()))
    wandb_worker_table = rank_worker(list(worker_status.values()))
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
    - No task: Status code 200, message {'message': 'no task to assign'} or {'message': 'all tasks are done'}
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
        return jsonify({'message': 'no task to assign'}), 200

    with lock:
        ranked_tasks = rank_task(list(task_status.values()))
        if ranked_tasks[0]['done_flag']:
            return jsonify({'message': 'all tasks are done'}), 200

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
        task_status[task_idx]['assigned_workers'].remove(worker_id)
        time_tmp = time.time()
        task_status[task_idx]['time_cost'] = time_tmp - task_status[task_idx]['last_update_time']
        task_status[task_idx]['last_update_time'] = time_tmp

        worker_status[worker_id]['reward'] += 1 if not task_has_done_flag else 0.000001
        worker_status[worker_id]['done_micro_tasks'].append(task_idx)
        worker_status[worker_id]['assigned_micro_task'] = None
        worker_status[worker_id]['last_update_time'] = time.time()
        sync_to_wandb()
    return jsonify({'message': 'task submitted successfully'}), 200

if __name__ == '__main__':
    logging.info("Starting the Flask app")
    init_tasks()
    app.run(debug=False, host='0.0.0.0', port=7290)

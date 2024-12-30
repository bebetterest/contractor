# Contractor 🚀

## Tool Overview 🛠️
This is a tool that allows multiple workers to register, be assigned tasks, and submit tasks. It uses Flask as the backend framework and integrates wandb for logging and monitoring. It is designed for the scenario where:
- a large number of separatable tasks need to be completed. (for example, inference LLM on massive amounts of instruction prompts)
- only transient and dynamic workers are available and they may be killed or restarted at anytime. Once a worker is killed, the storage is gone as well. (for example, low-priority jobs)

## Installation Steps 📦
1. Clone the repo to your local machine:
    ```bash
    git clone git@github.com:bebetterest/contractor.git
    cd contractor
    ```

2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Usage 🖥️

1. Start the Flask server:
    ```bash
    python contractor.py
    ```

2. Run the test script:
    ```bash
    python test_contractor.py
    ```

- (An application script is provided in `example_run_with_contractor.sh`)

## API Endpoints 🌐
### Register Worker 👷
- URL: `/worker_register`
- Method: `POST`
- Request Data:
    ```json
    {
        "worker_id": "string"  # Unique identifier for the worker
    }
    ```
- Response:
    - Success: Status code `200`, message `{'message': 'worker registered successfully'}`
    - Failure: Status code `400`, error message `{'error': 'worker_id is required'}` or `{'error': 'worker_id already registered, please use a different worker_id'}`

### Assign Task 📋
- URL: `/assign_task`
- Method: `POST`
- Request Parameters:
    - `worker_id`: Unique identifier for the worker
- Response:
    - Success: Status code `200`, task information `{"task_id": int, 'task': list}`
    - Failure: Status code `400`, error message `{'error': 'worker_id is required'}` or `{'error': 'worker_id not registered'}` or `{'error': 'worker already has a task assigned'}`
    - No Task: Status code `400`, message `{'message': 'no task to assign'}` or `{'message': 'all tasks are done'}`

### Submit Task ✅
- URL: `/submit_task`
- Method: `POST`
- Request Parameters:
    - `worker_id`: Unique identifier for the worker
- Response:
    - Success: Status code `200`, message `{'message': 'task submitted successfully'}`
    - Failure: Status code `400`, error message `{'error': 'worker_id is required'}` or `{'error': 'worker_id not registered'}` or `{'error': 'no task assigned to worker'}`

### Finish All Tasks 🏁
- URL: `/finish_all`
- Method: `POST`
- Response:
    - Success: Status code `200`, message `{'message': 'all tasks marked as done'}`

### Add Task ➕
- URL: `/add_task`
- Method: `POST`
- Request Data:
    ```json
    {
        "task_content": list  # The content of the new task
    }
    ```
- Response:
    - Success: Status code `200`, message `{'message': 'task added successfully', 'task_idx': new_task_idx}`
    - Failure: Status code `400`, error message `{'error': 'task_content is required'}`

## Acknowledgements 🙏
Thanks to vscode copilot for assistance with code and documentation.

---

enjoy:)

🤯betterest🤯
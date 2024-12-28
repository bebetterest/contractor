# Contractor 🚀

## Tool Overview 🛠️
This is a tool that allows multiple workers to register, be assigned tasks, and submit tasks. It uses Flask as the backend framework and integrates wandb for logging and monitoring.

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
    - No Task: Status code `200`, message `{'message': 'no task to assign'}` or `{'message': 'all tasks are done'}`

### Submit Task ✅
- URL: `/submit_task`
- Method: `POST`
- Request Parameters:
    - `worker_id`: Unique identifier for the worker
- Response:
    - Success: Status code `200`, message `{'message': 'task submitted successfully'}`
    - Failure: Status code `400`, error message `{'error': 'worker_id is required'}` or `{'error': 'worker_id not registered'}` or `{'error': 'no task assigned to worker'}`

## Acknowledgements 🙏
Thanks to vscode copilot for assistance with code and documentation.

enjoy:)

🤯betterest🤯


DATA_PATH=full_data.jsonl
SUB_DATA_PATH=sub_data.jsonl

CONTRACTOR_URL="http://127.0.0.1:7290"
RES_FOLDER="results"


if command -v rocm-smi &> /dev/null; then
    GPU_COUNT=$(rocminfo | grep 'Marketing Name' | grep 'AMD' | wc -l)
    GPU_TYPE=$(rocminfo | grep 'Marketing Name' | grep 'AMD' | head -n 1 | sed 's/.*: *//;s/ *$//;s/ /_/g')
else
    GPU_COUNT=$(nvidia-smi --query-gpu=gpu_name --format=csv,noheader | wc -l)
    GPU_TYPE=$(nvidia-smi --query-gpu=gpu_name --format=csv,noheader | head -n 1 | sed 's/.*: *//;s/ *$//;s/ /_/g')
fi

IP_ADDR=$(hostname -I | awk '{print $1}')
RANDOM_STR=$(cat /dev/urandom | LC_ALL=C tr -dc 'a-zA-Z0-9' | fold -w 4 | head -n 1)
WORKER_ID="${GPU_COUNT}x${GPU_TYPE}_${IP_ADDR}_${RANDOM_STR}"

echo "($WORKER_ID) registering worker"
while true; do
    STATUS_CODE=$(python3 connect_contractor.py --url $CONTRACTOR_URL --worker_id $WORKER_ID --type register | tail -n 1)
    if [ "$STATUS_CODE" -eq 200 ]; then
        echo "($WORKER_ID) worker registered successfully"
        break
    else
        echo "($WORKER_ID) registration failed, retrying in 15 seconds"
        sleep 60
    fi
done

while true; do

    echo "($WORKER_ID) assigning task"
    while true; do
        STATUS_CODE=$(
            python3 connect_contractor.py --url $CONTRACTOR_URL --worker_id $WORKER_ID --type assign | tail -n 1
        )
        if [ "$STATUS_CODE" -eq 200 ]; then
            echo "($WORKER_ID) task assigned successfully"
            break
        else
            echo "($WORKER_ID) task assignment failed, retrying in 15 seconds"
            sleep 60
        fi
    done


    echo "($WORKER_ID) start to cut the task data"
    while true; do
        # your custom code here
        TASK_ID=$(
            python3 cut_data.py --data $DATA_PATH --contractor_res contractor_res_json.log --output $SUB_DATA_PATH | tail -n 1
        )
        if [ $? -eq 0 ]; then
            echo "($WORKER_ID) task data cut successfully"
            break
        else
            echo "($WORKER_ID) task data cut failed, retrying in 60 seconds"
            sleep 60
        fi
    done


    echo "($WORKER_ID) (task_id: $TASK_ID) start to run"
    # your custom code here
    python3 main.py --input_file $SUB_DATA_PATH --output_file ${TASK_ID}_${WORKER_ID}.jsonl

    if [ $? -ne 0 ]; then
        echo "($WORKER_ID) (task_id: $TASK_ID) run failed"
        echo "exit now. please check the error message"
        exit 1
    fi


    echo "($WORKER_ID) (task_id: $TASK_ID) move the result now"
    if [ ! -d $RES_FOLDER ]; then
        mkdir -p $RES_FOLDER
    fi
    cp ${TASK_ID}_${WORKER_ID}.jsonl $RES_FOLDER


    echo "($WORKER_ID) (task_id: $TASK_ID) submit the task now"
    while true; do
        STATUS_CODE=$(
            python3 connect_contractor.py --url $CONTRACTOR_URL --worker_id $WORKER_ID --type submit | tail -n 1
        )
        if [ "$STATUS_CODE" -eq 200 ]; then
            echo "($WORKER_ID) (task_id: $TASK_ID) task submitted successfully"
            break
        else
            echo "($WORKER_ID) (task_id: $TASK_ID) task submission failed, retrying in 15 seconds"
            sleep 60
        fi
    done

done


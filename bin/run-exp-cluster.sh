#!/usr/bin/env bash

bin=$(dirname "$0")
bin=$(
  cd "$bin"
  pwd
)

. "$bin"/config.sh

TEST_TIME=${TEST_TIME:-$3}

run_exp() {
  if [[ ! -f $EXPERIMENTS_DIR/$1.yaml ]]; then
    echo "Experiment file does not exist"
    exit
  fi

  # Fetch the workload type
  local workload_type="unassigned"
  workload_type=$(yq r "$EXPERIMENTS_DIR/$1.yaml" "workload_type")
  cd "$BIN_DIR" || exit


  if [[ "$2" == "workload" ]] ; then
    ./start_workload.sh "$EXPERIMENTS_DIR/$1.yaml"
    echo "Sleeping for $TEST_TIME to let generator $1 run"
    sleep $TEST_TIME
    ./stop_workload.sh $workload_type
    echo "Workload $1 is done."
  elif [[ "$2" == "processing" ]]; then
    ./start_processing.sh "$EXPERIMENTS_DIR/$1.yaml"
    echo "Sleeping for $TEST_TIME to let generator $1 run"
    sleep $TEST_TIME
    ./stop_processing.sh "$workload_type"
    echo "Processing $1 is done."
  else
    echo "Unknown type of program of $1 you wish to run"
  fi

  sleep 3
}

if [[ $# -lt 3 ]]; then
  echo "3 Arguements Required: ./run-exp-tembo.sh <experiment_name> <workload|processing> <experiment_running_time>"
  exit
else
  while [[ $# -gt 0 ]]; do
    run_exp $1 $2
    shift
    shift
    shift
  done
fi

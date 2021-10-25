#!/usr/bin/env bash

bin=$(dirname "$0")
bin=$(
  cd "$bin"
  pwd
)

. "$bin"/config.sh

stop_zk() {
  $KAFKA_DIR/bin/zookeeper-server-stop.sh
  rm -rf /tmp/data/zk
}

stop_kafka() {
  stop_if_needed kafka\.Kafka Kafka
  rm -rf /tmp/kafka-logs/
}

stop_flink() {
  $FLINK_DIR/bin/stop-cluster.sh
}

stop_flink_processing_local() {
  local flink_id=$(
    $1 list | grep 'Flink Streaming Job' | awk '{print $4}'
    true
  )
  if [[ "$flink_id" == "" ]]; then
    echo "Could not find streaming job to kill"
  else
    $1 stop --savepoint /tmp/flink-savepoints $flink_id
    sleep 8
  fi
}

stop_flink_processing() {
  stop_flink_processing_local $FLINK_DIR/bin/flink
}

pull_stdout_from_flink_taskmanager() {
  local task_manager_id=-1
  task_manager_id=$(curl -s "http://localhost:8081/taskmanagers/" | jq '.taskmanagers[0].id' | tr -d '"')
  echo "Retrieving Output from TaskManager: $task_manager_id"
  curl -s "http://localhost:8081/taskmanagers/$task_manager_id/stdout" >"output_$1.txt"
  sleep 3
}

stop_sdv() {
  stop_if_needed sdv_server.py sdv
}

stop() {
  #stop_sdv
  stop_flink_processing
  sleep 5
  pull_stdout_from_flink_taskmanager $1

  stop_flink
  stop_kafka
  stop_zk
}

cd "$PROJECT_DIR" || exit
if [[ $# -lt 1 ]]; then
  echo "Invalid use: ./stop_processing.sh <experiment_name>"
else
  stop "$EXPERIMENTS_DIR/$1" # $1: experiment name, $2: Time to sleep
fi

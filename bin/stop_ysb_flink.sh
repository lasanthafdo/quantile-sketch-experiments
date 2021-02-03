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
  local curr_kafka_topic="$KAFKA_TOPIC_PREFIX-1"
  #"$KAFKA_DIR"/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic "$curr_kafka_topic"
  sleep 2
  #"$KAFKA_DIR"/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic "stragglers"
  sleep 2
  #"$KAFKA_DIR"/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic "stragglers-2"
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

pull_stdout() {
  local task_manager_id=$(curl -s "http://localhost:8081/taskmanagers/" | jq '.taskmanagers[0].id')
  echo "$task_manager_id"
  curl -s "http://localhost:8081/taskmanagers/$task_manager_id/stdout" >"$1_output.txt"
  sleep 3
}

pull_stdout_from_flink_taskmanager() {
  local task_manager_id=-1
  task_manager_id=$(curl -s "http://localhost:8081/taskmanagers/" | jq '.taskmanagers[0].id' | tr -d '"')
  echo "Retrieving Output from TaskManager: $task_manager_id"
  curl -s "http://localhost:8081/taskmanagers/$task_manager_id/stdout" >"$1_output.txt"
  sleep 3
}

stop_sdv() {
  stop_if_needed sdv_server.py sdv
}

stop() {
  stop_flink_processing
  sleep 5
  pull_stdout_from_flink_taskmanager $1

  stop_flink
  stop_kafka
  stop_zk
  stop_sdv
}

cd "$PROJECT_DIR"
if [[ $# -lt 1 ]]; then
  echo "Invalid use: ./stop_ysb_2.sh <experiment_name>"
elif [[ $# -lt 2 ]]; then
  stop "$EXPERIMENTS_DIR/$1"
else
  #stop_load
  stop "$EXPERIMENTS_DIR/$1" $2 # $1: experiment name, $2: Time to sleep
  echo "do nothing"
fi

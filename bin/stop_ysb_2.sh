#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

stop_zk(){
   $KAFKA_DIR/bin/zookeeper-server-stop.sh
}

stop_redis(){
   stop_if_needed redis-server Redis
   rm -f dump.rdb
}

stop_kafka(){
   for kafka_topic in `seq 1 $1`
   do
      local curr_kafka_topic="$KAFKA_TOPIC_PREFIX-$kafka_topic"
      "$KAFKA_DIR"/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic $curr_kafka_topic
   done
   stop_if_needed kafka\.Kafka Kafka
   rm -rf /tmp/kafka-logs/
}

stop_flink(){
    $FLINK_DIR/bin/stop-cluster.sh
}

stop_flink_processing_local() {
    local flink_id=`$1 list | grep 'Flink Streaming Job' | awk '{print $4}'; true`
    if [[ "$flink_id" == "" ]];
    then
        echo "Could not find streaming job to kill"
    else
         $1 stop --savepoint /tmp/flink-savepoints $flink_id
         sleep 8
    fi
}

stop_flink_processing(){
    stop_flink_processing_local $FLINK_DIR/bin/flink
}

pull_stdout() {
      local task_manager_id=`curl -s "http://localhost:8081/taskmanagers/" | jq '.taskmanagers[0].id'`
      echo "$task_manager_id"
      curl -s "http://localhost:8081/taskmanagers/$task_manager_id/stdout" > "$1_output.txt"
      sleep 3
}

pull_stdout_from_flink_taskmanager() {
      local task_manager_id=-1
      task_manager_id=$(curl -s "http://localhost:8081/taskmanagers/" | jq '.taskmanagers[0].id' | tr -d '"')
      echo  "Retrieving Output from TaskManager: $task_manager_id"
      curl -s "http://localhost:8081/taskmanagers/$task_manager_id/stdout" > "$1_output_after_stop_processing.txt"
      sleep 3
}

stop_load(){
   stop_if_needed "WorkloadGenerator" "WorkloadGenerator"
}

stop(){
    stop_load
    if [[ $# -gt 1 ]];
    then
        echo "Load Stopped. Sleeping for $2..."
        sleep $2
    fi
    stop_flink_processing
    sleep 5
    pull_stdout_from_flink_taskmanager $1

    local num_instances=$(yq r "$1.yaml" "num_instances")
    # stop_kafka $num_instances
    # stop_zk
    stop_redis
    stop_flink
}

cd "$PROJECT_DIR"
if [[ $# -lt 1 ]];
then
  echo "Invalid use: ./stop_ysb_2.sh <experiment_name>"
elif [[ $# -lt 2 ]];
then
  stop "$EXPERIMENTS_DIR/$1"
else
  stop "$EXPERIMENTS_DIR/$1" $2 # $1: experiment name, $2: Time to sleep
fi

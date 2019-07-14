#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

stop_zk(){
    $ZK_DIR/bin/zkServer.sh stop
}

stop_redis(){
    stop_if_needed redis-server Redis
    rm -f dump.rdb
}

stop_kafka(){
    for kafka_topic in `seq 1 $1`
    do
        local curr_kafka_topic="$KAFKA_TOPIC_PREFIX-$kafka_topic"
        $KAFKA_DIR/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic $curr_kafka_topic
    done
    stop_if_needed kafka\.Kafka Kafka
    rm -rf /tmp/kafka-logs/
}

stop_flink(){
    $FLINK_DIR/bin/stop-cluster.sh
}

stop_flink_processing(){
    local flink_id=`"$FLINK_DIR/bin/flink" list | grep 'Flink Streaming Job' | awk '{print $4}'; true`
    if [[ "$flink_id" == "" ]];
	then
	  echo "Could not find streaming job to kill"
    else
      "$FLINK_DIR/bin/flink" cancel $flink_id
      sleep 3
    fi
}

pull_stdout() {
      local task_manager_id=`curl -s "http://localhost:8081/taskmanagers/" | jq -r '.taskmanagers[0].id'`
      curl -s "http://localhost:8081/taskmanagers/$task_manager_id/stdout" > "$1_output.txt"
      sleep 3
}

stop_load(){
    stop_if_needed "WorkloadMain" "Workload Generator"
}

stop(){
    stop_load
    if [[ $# -gt 1 ]];
    then
        echo "Load Stopped. Sleeping..."
        sleep $2
        pull_stdout $1
    fi
    echo "Experiment $1 is being analyzed."
    ./analyze.sh $1
    stop_flink_processing
    stop_flink

    local num_instances=$(yaml "$1.yaml" "['num_instances']")
    stop_kafka $num_instances
    stop_redis
    stop_zk
}

cd "$PROJECT_DIR"
if [[ $# -lt 1 ]];
then
  echo "Invalid use: ./stop.sh <experiment_name>"
elif [[ $# -lt 2 ]];
then
  stop "$EXPERIMENTS_DIR/$1"
else
  stop "$EXPERIMENTS_DIR/$1" $2 # $1: experiment name, $2: Time to sleep
fi

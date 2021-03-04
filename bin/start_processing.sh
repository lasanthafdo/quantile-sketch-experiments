#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

start_zk(){
   $KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties &
   sleep 15
}

create_kafka_topic() {
    echo "Creating KAFKA Topic"
    local kafka_topic="$1"
    bash "$KAFKA_DIR"/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic "$1"
    echo "Finish Creating KAFKA Topic"
}


start_kafka(){
    echo "Starting KAFKA"
    start_if_needed kafka\.Kafka Kafka 10 "$KAFKA_DIR/bin/kafka-server-start.sh" "$KAFKA_DIR/config/server.properties"
    sleep 5
    create_kafka_topic "ad-events-1"
    create_kafka_topic "nyt-events"
    create_kafka_topic "stragglers"
    bash "$KAFKA_DIR"/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
    echo "Finish Starting KAFKA"
}

start_flink(){
    # flink starts on the second node
    echo "Starting Flink Cluster"
    start_if_needed org.apache.flink.runtime.jobmanager.JobManager Flink 1 $FLINK_DIR/bin/start-cluster.sh
    sleep 10
    echo "Finish Starting Flink Cluster"
}

start_flink_processing(){
    echo "Deploying Flink Client"
    # bash "$FLINK_DIR/bin/flink run --detached $WORKLOAD_PROCESSOR_JAR_FILE --setup $SETUP_FILE --experiment $1 &"
    "$FLINK_DIR/bin/flink" run "$WORKLOAD_PROCESSOR_JAR_FILE" --setup $SETUP_FILE --experiment "$1" &
    sleep 5
    echo "Finish Deploying Flink Client"
}

start_sdv(){
  #source "$HOME"/PythonEnvironments/sdv_project/bin/activate
  echo "Starting SDV"
  cd "$PROJECT_DIR"/sdv || echo "Cannot cd into sdv folder" || exit
  nohup python sdv_server.py data.csv &
  nohup python create_new_model.py data.csv &
  cd "$PROJECT_DIR" || exit
  echo "Finish starting SDV"
}

start(){
    echo $1

    hname=$(hostname)
    if [[ $hname == "Harshs-MBP"  ]]; then
      maven_clean_install_with_tests $PROJECT_DIR/workload-processor-flink
    fi
    start_zk
    start_kafka
    start_flink
    #start_sdv
    read -n 1 -s -r -p "Press any key to continue"
    start_flink_processing $1
}

if [[ $# -lt 1 ]];
then
  echo "Invalid use: ./start_ysb.sh <experiment_name>"
else
  cd "$PROJECT_DIR" || exit
  start "$1" # $1: experiment file
fi
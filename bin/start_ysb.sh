#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

start_zk(){
   $KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties &
   sleep 15
}

start_redis(){
   echo "Starting Redis"
   start_if_needed redis-server Redis 1 "$REDIS_DIR/src/redis-server"
   echo "Finish Starting Redis"

   cd $WORKLOAD_GENERATOR_DIR
   $MVN exec:java -Dexec.mainClass="WorkloadGeneratorEntryPoint" -Dexec.args="-s $SETUP_FILE -e $1 -n"
   cd $PROJECT_DIR
}

create_kafka_topic() {
    echo "Creating KAFKA Topic"
    bash "$KAFKA_DIR"/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic "$1"
}


start_kafka(){
    echo "Starting KAFKA"
    start_if_needed kafka\.Kafka Kafka 10 "$KAFKA_DIR/bin/kafka-server-start.sh" "$KAFKA_DIR/config/server.properties"
    sleep 5
    create_kafka_topic "ad-events-1"
    create_kafka_topic "ad-events-2"
    create_kafka_topic "stragglers"
    create_kafka_topic "stragglers-2"
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
    "$FLINK_DIR/bin/flink" run "$WORKLOAD_PROCESSOR_JAR_FILE" --setup $SETUP_FILE --experiment $1 &
    sleep 5
    echo "Finish Deploying Flink Client"
}

start_sdv(){
  source "$HOME"/PythonEnvironments/sdv_project/bin/activate
  cd "$PROJECT_DIR"/sdv
  nohup python sdv_server.py &
  cd "$PROJECT_DIR"
}


start_load(){
    # flink_load starts on the first node
    echo "Start Workload Generator"
    cd "$WORKLOAD_GENERATOR_DIR"
    mvn exec:java -Dexec.mainClass="WorkloadGeneratorEntryPoint" -Dexec.args="-s $SETUP_FILE -e $1 -r" &
    cd "$PROJECT_DIR"
}

start(){
    echo $1

    maven_clean_install_with_tests $PROJECT_DIR
    local num_instances=-1
    num_instances=$(yq r $1 "num_instances")

    start_zk
    start_redis $1
    start_kafka $num_instances
    start_flink
    #start_sdv
    start_flink_processing $1
    start_load $1
}

if [[ $# -lt 1 ]];
then
  echo "Invalid use: ./start_ysb.sh <experiment_name>"
else
  cd "$PROJECT_DIR"
  start $1 # $1: experiment file
fi
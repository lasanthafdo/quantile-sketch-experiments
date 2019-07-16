#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

start_zk(){
  $ZK_DIR/bin/zkServer.sh start
}

start_redis(){
    start_if_needed redis-server Redis 1 "$REDIS_DIR/src/redis-server"
    cd $WORKLOAD_GENERATOR_DIR
    $MVN exec:java -Dexec.mainClass="WorkloadMain" -Dexec.args="-s $SETUP_FILE -e $1 -n"
    cd $PROJECT_DIR
}

create_kafka_topic() {
    for kafka_topic in `seq 1 $1`
    do
        local curr_kafka_topic="$KAFKA_TOPIC_PREFIX-$kafka_topic"
        echo $curr_kafka_topic
        local count=`$KAFKA_DIR/bin/kafka-topics.sh --describe --zookeeper "$ZK_CONNECTIONS" --topic $curr_kafka_topic 2>/dev/null | grep -c $curr_kafka_topic`
        if [[ "$count" = "0" ]];
        then
            $KAFKA_DIR/bin/kafka-topics.sh --create --zookeeper "$ZK_CONNECTIONS" --replication-factor 1 --partitions 1 --topic $curr_kafka_topic
        else
            echo "Kafka topic $curr_kafka_topic already exists"
        fi
    done
}

start_kafka(){
    start_if_needed kafka\.Kafka Kafka 10 "$KAFKA_DIR/bin/kafka-server-start_ysb.sh" "$KAFKA_DIR/config/server.properties"
    create_kafka_topic $1
}

start_flink(){
    start_if_needed org.apache.flink.runtime.jobmanager.JobManager Flink 1 $FLINK_DIR/bin/start-cluster.sh
    sleep 5
}

start_flink_processing(){
    echo "Deploying Flink Client"
    "$FLINK_DIR/bin/flink" run $WORKLOAD_PROCESSOR_JAR_FILE --setup $SETUP_FILE --experiment $1 &
    sleep 5
}

start_load(){
    cd $WORKLOAD_GENERATOR_DIR
    $MVN exec:java -Dexec.mainClass="WorkloadMain" -Dexec.args="-s $SETUP_FILE -e $1 -r" &
    cd $PROJECT_DIR
}

start(){
    echo $1

    maven_clean_install_with_tests $PROJECT_DIR
    local num_instances=$(yaml $1 "['num_instances']")

    start_zk
    start_redis $1
    start_kafka $num_instances
    start_flink
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

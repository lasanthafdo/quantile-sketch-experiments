#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

start_zk(){
 if [[ $HAS_HOSTS ]];
 then
    while read line
	do
        echo "Starting ZK on $line"
        ssh ${line} "$ZK_DIR/bin/zkServer.sh start /tmp/data/zk/zoo.cfg" </dev/null
    done <${HOSTS_FILE}
 else
    $ZK_DIR/bin/zkServer.sh start
 fi
}

start_redis(){
    ## TODO(oibfarhat): Make it distributed, if that is even possible.
    start_if_needed redis-server Redis 1 "$REDIS_DIR/src/redis-server"
    cd $WORKLOAD_GENERATOR_DIR
    $MVN exec:java -Dexec.mainClass="WorkloadMain" -Dexec.args="-s $SETUP_FILE -e $1 -n"
    cd $PROJECT_DIR
}

create_kafka_topic() {
    for kafka_topic in `seq 1 $1`
    do
        local curr_kafka_topic="$KAFKA_TOPIC_PREFIX-$kafka_topic"
        if [[ $HAS_HOSTS ]];
        then
            ssh $2 "
		$(declare -f start_if_needed);
		$(declare -f pid_match);
		    local count=`$KAFKA_DIR/bin/kafka-topics.sh --describe --zookeeper "$ZK_CONNECTION" --topic $curr_kafka_topic 2>/dev/null | grep -c $curr_kafka_topic`
            if [[ $count -eq 0 ]];
            then
                $KAFKA_DIR/bin/kafka-topics.sh --create --zookeeper $ZK_CONNECTION --replication-factor 1 --partitions 1 --topic $curr_kafka_topic
            else
                echo 'Kafka topic $curr_kafka_topic already exists'
            fi" </dev/null
        else
          local count=`$KAFKA_DIR/bin/kafka-topics.sh --describe --zookeeper "$ZK_CONNECTION" --topic $curr_kafka_topic 2>/dev/null | grep -c $curr_kafka_topic`
          if [[ "$count" = "0" ]];
          then
            $KAFKA_DIR/bin/kafka-topics.sh --create --zookeeper "$ZK_CONNECTION" --replication-factor 1 --partitions 1 --topic $curr_kafka_topic
            else
                echo "Kafka topic $curr_kafka_topic already exists"
            fi
        fi
    done
}

start_kafka(){
    echo "Starting KAFKA"
    if [[ $HAS_HOSTS ]];
    then
        while read line
	    do
            echo "Starting Kafka on $line"
            ssh ${line} "
		$(declare -f start_if_needed);
		$(declare -f pid_match);
                start_if_needed kafka\.Kafka Kafka 10 '$KAFKA_DIR/bin/kafka-server-start.sh' '/tmp/data/server.properties'
                " </dev/null
                create_kafka_topic $1 $line
        done <${HOSTS_FILE}
    else
        start_if_needed kafka\.Kafka Kafka 10 "$KAFKA_DIR/bin/kafka-server-start.sh" "$KAFKA_DIR/config/server.properties"
        create_kafka_topic $1
    fi
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

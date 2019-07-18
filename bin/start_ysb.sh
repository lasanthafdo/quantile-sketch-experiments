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
        ssh_connect ${line} "$ZK_DIR/bin/zkServer.sh start /tmp/data/zk/zoo.cfg" 10
    done <${HOSTS_FILE}
 else
    $ZK_DIR/bin/zkServer.sh start
 fi
}

start_redis(){
    if [[ $HAS_HOSTS ]];
    then

        # Launch Redis on all instances
        while read line
        do
            echo "Launching redis on $line"
            ssh -f ${line} "
            $(declare -f start_if_needed);
            $(declare -f pid_match);
            start_if_needed redis-server Redis 1 $REDIS_DIR/src/redis-server"
            sleep 10
        done <${HOSTS_FILE}

       # Setup new campaigns on the first node
        first_node=""
        while read line
        do
            first_node=$line
            break
        done <${HOSTS_FILE}

        echo "Deploying Redis on $first_node"
        ssh_connect ${first_node} "$(declare -f start_if_needed); $(declare -f pid_match); cd $WORKLOAD_GENERATOR_DIR; $MVN exec:java -Dexec.mainClass='WorkloadMain' -Dexec.args='-s $SETUP_FILE -e $1 -n'" 20
    else
        start_if_needed redis-server Redis 1 "$REDIS_DIR/src/redis-server"
        cd $WORKLOAD_GENERATOR_DIR
        $MVN exec:java -Dexec.mainClass="WorkloadMain" -Dexec.args="-s $SETUP_FILE -e $1 -n"
        cd $PROJECT_DIR
    fi
}

create_kafka_topic() {
    for kafka_topic in `seq 1 $1`
    do
        local curr_kafka_topic="$KAFKA_TOPIC_PREFIX-$kafka_topic"
        if [[ $HAS_HOSTS ]];
        then
	        echo "creating $curr_kafka_topic at $2"
            ssh_connect $2 "$KAFKA_DIR/bin/kafka-topics.sh --create --zookeeper $ZK_CONNECTION --replication-factor 1 --partitions 1 --topic $curr_kafka_topic" 5
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
            ssh -f ${line} "
                $(declare -f start_if_needed);
                $(declare -f pid_match);
                start_if_needed kafka\.Kafka Kafka 10 '$KAFKA_DIR/bin/kafka-server-start.sh' '/tmp/data/server.properties'"
            sleep 30
            echo "Creating topics now for $line"
            create_kafka_topic $1 $line
        done <${HOSTS_FILE}
    else
        start_if_needed kafka\.Kafka Kafka 10 "$KAFKA_DIR/bin/kafka-server-start.sh" "$KAFKA_DIR/config/server.properties"
        create_kafka_topic $1
    fi
}

start_flink(){
    # flink starts on the second node
    if [[ $HAS_HOSTS ]];
    then
        second_node=""
        while read line
        do
            second_node=$line
        done <${HOSTS_FILE}

        ssh_connect ${second_node} "
            $(declare -f start_if_needed);
            $(declare -f pid_match);
            start_if_needed org.apache.flink.runtime.jobmanager.JobManager Flink 1 $FLINK_DIR/bin/start-cluster.sh" 10

        sleep 10
    else
        start_if_needed org.apache.flink.runtime.jobmanager.JobManager Flink 1 $FLINK_DIR/bin/start-cluster.sh
        sleep 5
    fi
}

start_flink_processing(){
    # flink_processing starts on the second node
    if [[ $HAS_HOSTS ]];
    then
        second_node=""
        while read line
        do
            second_node=$line
        done <${HOSTS_FILE}

        echo "Deploying Flink Client on $second_node"

        ssh_connect ${second_node} "$FLINK_DIR/bin/flink run $WORKLOAD_PROCESSOR_JAR_FILE --setup $SETUP_FILE --experiment $1 &" 10
        sleep 10
    else
        echo "Deploying Flink Client"
        "$FLINK_DIR/bin/flink" run $WORKLOAD_PROCESSOR_JAR_FILE --setup $SETUP_FILE --experiment $1 &
        sleep 5
    fi
}

start_load(){
    # flink_load starts on the first node
    if [[ $HAS_HOSTS ]];
    then
        first_node=""
        while read line
        do
            first_node=$line
            break
        done <${HOSTS_FILE}

        echo "Deploying Load on $first_node"

        ssh_connect ${first_node} "
            cd $WORKLOAD_GENERATOR_DIR
            $MVN exec:java -Dexec.mainClass='WorkloadMain' -Dexec.args='-s $SETUP_FILE -e $1 -r' &" 10
    else
        cd $WORKLOAD_GENERATOR_DIR
        $MVN exec:java -Dexec.mainClass="WorkloadMain" -Dexec.args="-s $SETUP_FILE -e $1 -r" &
        cd $PROJECT_DIR
    fi
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

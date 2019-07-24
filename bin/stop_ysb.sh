#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

stop_zk(){
    if [[ $HAS_HOSTS -eq 1 ]];
    then
    	while read line
	    do
		    echo "Stopping ZK on $line"
		    ssh ${line} "$ZK_DIR/bin/zkServer.sh stop /tmp/data/zk/zoo.cfg" </dev/null
	    done <${HOSTS_FILE}
    else
    	$ZK_DIR/bin/zkServer.sh stop
    fi
}

stop_redis(){
    if [[ $HAS_HOSTS -eq 1 ]];
    then
        first_node=""
        while read line
        do
        ssh ${line} "
            $(declare -f stop_if_needed);
            $(declare -f pid_match);
            stop_if_needed redis-server Redis
            rm -f dump.rdb" </dev/null
        done <${HOSTS_FILE}
    else
        stop_if_needed redis-server Redis
        rm -f dump.rdb
    fi
}

stop_kafka(){
    if [[ $HAS_HOSTS -eq 1 ]];
    then
        while read line
	do
		echo "Stopping Kafka on $line"
		counter=0
		for kafka_topic in `seq 1 $1`
		do
			curr_kafka_topic="$KAFKA_TOPIC_PREFIX-$kafka_topic"
			ssh ${line} "
				echo $curr_kafka_topic
				$KAFKA_DIR/bin/kafka-topics.sh --zookeeper $ZK_CONNECTION --delete --topic $curr_kafka_topic
			" </dev/null
		done
		ssh ${line} "
		$(declare -f stop_if_needed);
		$(declare -f pid_match);
		stop_if_needed kafka\.Kafka Kafka
		rm -rf /tmp/kafka-logs/
		" </dev/null
	done <${HOSTS_FILE}
    else
	for kafka_topic in `seq 1 $1`
    	do
        	local curr_kafka_topic="$KAFKA_TOPIC_PREFIX-$kafka_topic"
        	$KAFKA_DIR/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic $curr_kafka_topic
   	 done
   	 stop_if_needed kafka\.Kafka Kafka
   	 rm -rf /tmp/kafka-logs/
    fi
}

stop_flink(){
    # flink starts on the second node
    if [[ $HAS_HOSTS -eq 1 ]];
    then
        second_node=""
        while read line
        do
            second_node=$line
        done <${HOSTS_FILE}

        ssh ${second_node} "
            $(declare -f start_if_needed);
            $(declare -f pid_match);
            $FLINK_DIR/bin/stop-cluster.sh" </dev/null
    else
        $FLINK_DIR/bin/stop-cluster.sh
    fi
}

stop_flink_processing_local() {
    local flink_id=`$1 list | grep 'Flink Streaming Job' | awk '{print $4}'; true`
    if [[ "$flink_id" == "" ]];
    then
        echo "Could not find streaming job to kill"
    else
         $1 cancel $flink_id
         sleep 3
    fi
}

stop_flink_processing(){
    # flink starts on the second node
    if [[ $HAS_HOSTS -eq 1 ]];
    then
        second_node=""
        while read line
        do
            second_node=$line
        done <${HOSTS_FILE}

        ssh ${second_node} "
            $(declare -f stop_flink_processing_local);
            stop_flink_processing_local $FLINK_DIR/bin/flink" </dev/null
        sleep 3
    else
        stop_flink_processing_local $FLINK_DIR/bin/flink
    fi
}

pull_stdout() {
      local task_manager_id=`curl -s "http://localhost:8081/taskmanagers/" | jq -r '.taskmanagers[0].id'`
      curl -s "http://localhost:8081/taskmanagers/$task_manager_id/stdout" > "$1_output.txt"
      sleep 3
}

stop_load(){
    if [[ $HAS_HOSTS -eq 1 ]];
    then
        first_node=""
        while read line
        do
            first_node=$line
            break
        done <${HOSTS_FILE}
        ssh ${first_node} "
            $(declare -f stop_if_needed);
            $(declare -f pid_match);
            stop_if_needed 'WorkloadMain' 'Workload Generator'" </dev/null
    else
        stop_if_needed "WorkloadMain" "Workload Generator"
    fi
}

stop(){
    stop_load
    if [[ $# -gt 1 ]];
    then
        echo "Load Stopped. Sleeping..."
        sleep $2
        pull_stdout $1
    fi
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
  echo "Invalid use: ./stop_ysb.sh <experiment_name>"
elif [[ $# -lt 2 ]];
then
  stop "$EXPERIMENTS_DIR/$1"
else
  stop "$EXPERIMENTS_DIR/$1" $2 # $1: experiment name, $2: Time to sleep
fi

#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh


init_zk_multinodes_conf(){
    # zk multi-nodes
    echo "Setting up ZK confs"
    if [[ $HAS_HOSTS ]]; then
	local counter=0
	while read line
	do
            ((counter++))
            echo "SSH-ing to $line..."

            ssh ${line} "
                if [[ ! -d /tmp/data ]]; then
                    mkdir /tmp/data
                fi

                if [[ ! -d /tmp/data/zk ]]; then
                    mkdir /tmp/data/zk
                fi
                echo $counter > /tmp/data/zk/myid
		"</dev/null
            echo "Wrote myId=$counter to $line, leaving..."
    done <${HOSTS_FILE}


    # Now copy these files to /tmp/
    while read line
	do
        ((counter++))
        echo "SSH-ing to $line..."

        ssh ${line} "
            mv $ZK_CONF_FILE /tmp/data/zk/zoo.cfg
            sed -i "s/server.${counter}=.*/server.${counter}=0.0.0.0:2888:3888/g" /tmp/data/zk/zoo.cfg
		 "</dev/null
        echo "Moved zoo.cfg"
        done <${HOSTS_FILE}

    fi
}

init_kafka_multinodes_conf(){
    # zk multi-nodes
    echo "Setting up KAFKA confs"
    if [[ $HAS_HOSTS ]]; then
	local counter=0
	while read line
	do
            ((counter++))
            echo "SSH-ing to $line..."
            ssh ${line} "
                cp $HOME/klink-benchmarks/benchmark/kafka/config/server.properties /tmp/data/server.properties
                sed -i "s/broker.id=.*/broker.id=$counter/g" /tmp/data/server.properties
		"</dev/null
            echo "Changed broker.id=$counter to $line, leaving..."
        done <${HOSTS_FILE}
    fi
}
setup(){
    init_zk_multinodes_conf
    init_kafka_multinodes_conf
}

setup

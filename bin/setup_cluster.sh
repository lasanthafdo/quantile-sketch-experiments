#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh


init_zk_multinodes_conf(){
    # zk multi-nodes
    if [[ $HAS_HOSTS ]]; then
        local counter=0
        while read line
        do
           ((counter++))
           echo "server.$counter=$line:2888:3888" >> $ZK_CONF_FILE
        done <${HOSTS_FILE}

        local counter=0
        while read line
        do
            ((counter++))
            echo "SSH-ing to $line..."
            ssh -T ${line} "
            if [[ ! -d /tmp/data ]]; then
                mkdir /tmp/data
            fi

            if [[ ! -d /tmp/data/zk ]]; then
                mkdir /tmp/data/zk
            fi

            echo $counter > /tmp/data/zk/myid
            "
            echo "Wrote myId=$counter to $line, leaving..."
        done <${HOSTS_FILE}
    fi
}

setup(){
    init_zk_multinodes_conf
}

setup

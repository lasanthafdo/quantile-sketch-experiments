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
    fi
}

setup(){
    init_zk_multinodes_conf
}

setup

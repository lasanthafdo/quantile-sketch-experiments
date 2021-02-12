#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

stop_redis(){
   stop_if_needed redis-server Redis
   rm -f dump.rdb
}

stop_load(){
   stop_if_needed "WorkloadGenerator" "WorkloadGenerator"
}

stop(){
    stop_load
    #stop_redis
}

cd "$PROJECT_DIR" || exit
stop

#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

TEST_TIME=${TEST_TIME:-240}

run_exp(){
    ## Verify experiment exists
    if [[ ! -f $EXPERIMENTS_DIR/$1.yaml ]];
	then
	    echo "Experiment file does not exist"
	    exit -1
	fi

	# Fetch the workload type
	local workload_type=$(yaml $1 "['workload_type']")
    cd $BIN_DIR

    if [[ "$workload_type" = "YSB" ]]; then
        echo "Running YSB Experiment"
        # Run YSB
        ./YSB/start_ysb.sh "$EXPERIMENTS_DIR/$1.yaml"
        sleep $TEST_TIME

        # Stop YSB
        ./YSB/stop_ysb.sh $1 10
    elif [[ "$workload_type" = "LRB" ]]; then
        echo "Running LRB Experiment"
         # Run LRB

         # Stop LRB
    fi

    echo "$Experiment $1 is done."
    sleep 60
}


if [[ $# -lt 1 ]];
then
  echo "Invalid use: ./run-exp.sh <experiment_name>"
else
    while [[ $# -gt 0 ]];
    do
        run_exp $1
        shift
    done
fi

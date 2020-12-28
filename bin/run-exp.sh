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
	local workload_type=$(yq r "$EXPERIMENTS_DIR/$1.yaml" "workload_type")
    cd $BIN_DIR

    if [[ "$workload_type" = "ysb" ]]; then
        echo "Running YSB Experiment"
        # Run YSB
        ./start_ysb.sh "$EXPERIMENTS_DIR/$1.yaml"
        echo "Sleeping for $TEST_TIME to let experiment run"
        sleep $TEST_TIME

        # Stop YSB
        ./stop_ysb.sh $1 10
    elif [[ "$workload_type" = "lrb" ]]; then
        echo "Running LRB Experiment"
        # Run LRB
        ./start_lrb.sh "$EXPERIMENTS_DIR/$1.yaml"
        sleep $TEST_TIME

        # Stop LRB
        ./stop_lrb.sh $1 10
    else
        echo "Unknown Workload!"
    fi

    echo "$Experiment $1 is done."
    sleep 20
}


if [[ $# -lt 1 ]];
then
  echo "Invalid use: ./run-exp.sh <experiment_names>"
else
    while [[ $# -gt 0 ]];
    do
        run_exp $1
        shift
    done
fi

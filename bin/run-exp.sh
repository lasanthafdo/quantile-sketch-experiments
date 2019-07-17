#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

TEST_TIME=${TEST_TIME:-240}

run_exp(){
    # Verify first experiment exists
    if [[ ! -f $EXPERIMENTS_DIR/$1.yaml ]];
	then
	    echo "Experiment file does not exist"
	    exit -1
	fi

    cd $BIN_DIR
    echo "Experiment $1.yaml has started."
    ./start.sh "$EXPERIMENTS_DIR/$1.yaml"
    sleep $TEST_TIME
    ./stop.sh $1 10
    echo "Experiment $1 is being analyzed."
    # ./analyze.sh $1
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

#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

gen_exp(){
    if [[ ! -d $EXPERIMENTS_DIR ]]; then
        mkdir $EXPERIMENTS_DIR
    fi

    cd $EXPERIMENTS_DIR

    local exp_file="$1.yaml"
    if [[ -e "$exp_file" ]];
    then
        echo "experiment already exists!"
        exit
    fi
    # experiment file
    echo 'experiment_name:' $1 > $exp_file
    echo 'benchmark_name:' $2 > $exp_file
    echo 'algorithm_index:' $3 >> $exp_file
    echo 'num_instances:' $4 >> $exp_file
    echo 'throughput:' $5 >> $exp_file
    echo 'watermark_frequency:' $6 >> $exp_file
    echo 'window_size:' $7 >> $exp_file
}

if [[ $# -lt 5 ]];
then
  echo "Invalid use: gen-exp.sh <experiment_name> <benchmark_name> <algorithm_index> <num_instances> <throughput> <watermark_frequency> <window_size_in_seconds>"
else
    gen_exp $1 $2 $3 $4 $5 $6
fi

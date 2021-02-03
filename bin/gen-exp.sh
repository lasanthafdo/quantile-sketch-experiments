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
    echo 'workload_type:' $2 >> $exp_file
    echo 'num_instances:' $3 >> $exp_file
    echo 'throughput:' $4 >> $exp_file
    echo 'watermark_frequency:' $5 >> $exp_file
    echo 'window_size:' $6 >> $exp_file
}

if [[ $# -lt 6 ]];
then
  echo "Less than 6 number of arguments"
  echo "Using defaults: gen-exp.sh experiment_name: ysb_default workload_type: ysb num_instances: 1 throughput: 500events/s watermark_freq: 400ms window_size_in_seconds: 3"
  gen_exp ysb_default ysb 1 500 200 3
else
  gen_exp $1 $2 $3 $4 $5 $6
fi

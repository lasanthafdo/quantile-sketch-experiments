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
    echo 'algorithm:' $7 >> $exp_file
    echo 'missing_data: false' >> $exp_file
}

if [[ $# -lt 7 ]];
then
  echo "Less than 7 number of arguments"
  echo "Using defaults: gen-exp.sh experiment_name: synu_default workload_type: synu num_instances: 1 throughput: 50000 events/s watermark_freq: 5s window_size_in_seconds: 20 algorithm: ddsketch"
  gen_exp synu_default synu 1 50000 5000 20 ddsketch
else
  echo "Using supplied arguments"
  echo "gen-exp.sh <experiment_name>  <workload_type> <num_instances> <throughput>  <watermark_freq>  <window_size_in_seconds> <algorithm>"
  gen_exp $1 $2 $3 $4 $5 $6 $7
fi

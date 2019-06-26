#!/usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

analyze_exp(){
    cd $WORKLOAD_ANALYZER_DIR
    $MVN exec:java -Dexec.mainClass="WorkloadOutputAnalyzer" -Dexec.args="$1"
}

if [[ $# -lt 1 ]];
then
  echo "Invalid use: ./analyze.sh <experiment_name>"
else
    maven_clean_install_with_tests $WORKLOAD_ANALYZER_DIR
    while [[ $# -gt 0 ]];
    do
        analyze_exp $1
        shift
    done
fi

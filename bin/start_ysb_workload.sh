#!/bin/bash

bin=$(dirname "$0")
bin=$(
  cd "$bin"
  pwd
)

. "$bin"/config.sh

start_redis() {
  local PID=$(pid_match "redis-server")
  if [[ -n "$PID" ]]; then
    echo "Redis already running"
  elif [[ -f dump.rdb  ]]; then
    echo "Starting Redis"
    start_if_needed redis-server Redis 1 "$REDIS_DIR/src/redis-server" "--protected-mode no"
    echo "Finish Starting Redis"
  else
    start_if_needed redis-server Redis 1 "$REDIS_DIR/src/redis-server" "--protected-mode no"
    cd "$WORKLOAD_GENERATOR_DIR" || exit
    mvn exec:java -Dexec.mainClass="WorkloadGeneratorEntryPoint" -Dexec.args="-s $SETUP_FILE -e $1 -n"
    cd "$PROJECT_DIR" || exit
  fi
}

start_load() {
  # flink_load starts on the first node
  echo "Start Workload Generator"
  cd "$WORKLOAD_GENERATOR_DIR" || exit
  mvn exec:java -Dexec.mainClass="WorkloadGeneratorEntryPoint" -Dexec.args="-s $SETUP_FILE -e $1 -r" &
  cd "$PROJECT_DIR" || exit
}

start() {
  hname=$(hostname)
  if [[ $hname == "Harshs-MBP"  ]]; then
    maven_clean_install_no_tests $PROJECT_DIR/workload-generator
  fi

  start_redis $1
  read -n 1 -s -r -p "Press any key to continue"
  start_load $1
}

if [[ $# -lt 1 ]]; then
  echo "Invalid use: ./start_ysb.sh <experiment_name>"
else
  cd "$PROJECT_DIR" || exit
  start $1 # $1: experiment file
fi

#!/usr/bin/env bash

# Project Level
PROJECT_DIR="$HOME/streaming-benchmarks"
KLINK_DIR="$HOME/klink"

# Current Project Level
BIN_DIR="$PROJECT_DIR/bin"
BENCH_DIR="$PROJECT_DIR/benchmark"
SETUP_FILE="$PROJECT_DIR/setup.yaml"

WORKLOAD_GENERATOR_DIR="$PROJECT_DIR/workload-generator"
WORKLOAD_PROCESSOR_DIR="$PROJECT_DIR/workload-processor-flink"
WORKLOAD_ANALYZER_DIR="$PROJECT_DIR/workload-analyzer"

# Benchmark Level
EXPERIMENTS_DIR="$BENCH_DIR/experiments"
DOWNLOAD_CACHE_DIR="$BENCH_DIR/download-cache"
ZK_DIR="$BENCH_DIR/zk"
REDIS_DIR="$BENCH_DIR/redis"
KAFKA_DIR="$BENCH_DIR/kafka"
FLINK_DIR="$BENCH_DIR/flink"
FLINK_SRC_DIR="$BENCH_DIR/flink-src"
FLINK_CONF_FILE="$FLINK_DIR/conf/flink-conf.yaml"

# FILES
WORKLOAD_PROCESSOR_JAR_FILE="$WORKLOAD_PROCESSOR_DIR/target/workload-processor-flink-0.5.0.jar"

# ZK Parameters
ZK_HOST="localhost"
ZK_PORT="2181"
ZK_CONNECTIONS="$ZK_HOST:$ZK_PORT"

# KAFKA Parameters
KAFKA_TOPIC_PREFIX="ad-events"

# Commands
MAKE=${MAKE:-make}
GIT=${GIT:-git}
MVN=${MVN:-mvn}

pid_match() {
   local VAL=`ps -aef | grep "$1" | grep -v grep | awk '{print $2}'`
   echo $VAL
}

start_if_needed() {
  local match="$1"
  shift
  local name="$1"
  shift
  local sleep_time="$1"
  shift
  local PID=`pid_match "$match"`

  if [[ "$PID" -ne "" ]];
  then
    echo "$name is already running..."
  else
    "$@" &
    sleep $sleep_time
  fi
}

stop_if_needed() {
  local match="$1"
  local name="$2"
  local PID=`pid_match "$match"`
  if [[ "$PID" -ne "" ]];
  then
    kill "$PID"
    sleep 1
    local CHECK_AGAIN=`pid_match "$match"`
    if [[ "$CHECK_AGAIN" -ne "" ]];
    then
      kill -9 "$CHECK_AGAIN"
    fi
  else
    echo "No $name instance found to stop"
  fi
}

fetch_untar_file() {
  local file="$DOWNLOAD_CACHE_DIR/$1"
  local url=$2
  if [[ -e "$file" ]];
  then
    echo "Using cached File $file"
  else
	mkdir -p $DOWNLOAD_CACHE_DIR
    WGET=`whereis wget`
    CURL=`whereis curl`
    if [[ -n "$WGET" ]];
    then
      wget -O "$file" "$url"
    elif [[ -n "$CURL" ]];
    then
      curl -o "$file" "$url"
    else
      echo "Please install curl or wget to continue.";
      exit 1
    fi
  fi
  tar -xzvf $file
}

maven_clean_install_with_tests(){
    cd $1
    $MVN clean install -Dcheckstyle.skip -Drat.skip=true
}

maven_clean_install_no_tests(){
    cd $1
    $MVN clean install -DskipTests -Dcheckstyle.skip -Drat.skip=true
}

yaml() {
    python3 -c "import yaml;print(yaml.load(open('$1'))$2)"
}

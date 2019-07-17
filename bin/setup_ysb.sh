#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

# Apache Mirror
APACHE_MIRROR="https://archive.apache.org/dist"

# Scala parameters
SCALA_BIN_VERSION=${SCALA_BIN_VERSION:-"2.12"}

# Redis download parameters
REDIS_VERSION=${REDIS_VERSION:-"5.0.5"}

# Kafka download parameters
KAFKA_VERSION=${KAFKA_VERSION:-"2.2.0"}
KAFKA_BROKERS="localhost"
KAFKA_PARTITIONS=${KAFKA_PARTITIONS:-1}

init_setup_file(){
    # setup file
    echo 'kafka.brokers:' > $SETUP_FILE
    echo '    - "'$KAFKA_BROKERS'"' >> $SETUP_FILE
    echo >> $SETUP_FILE
    echo 'zookeeper.servers:' >> $SETUP_FILE
    echo '    - "'$ZK_HOST'"' >> $SETUP_FILE
    echo >> $SETUP_FILE
    echo 'kafka.port: 9092' >> $SETUP_FILE
    echo 'zookeeper.port: '$ZK_PORT >> $SETUP_FILE
    echo 'redis.host: "localhost"' >> $SETUP_FILE
    echo 'kafka.topic.prefix: "'$KAFKA_TOPIC_PREFIX'"' >> $SETUP_FILE
    echo 'kafka.partitions: '$KAFKA_PARTITIONS >> $SETUP_FILE
}

init_redis(){
    # Fetch Redis
    if [[ ! -d "redis" ]]; then
        local redis_file="redis-$REDIS_VERSION"
        local redis_tar_file="$redis_file.tar.gz"
        fetch_untar_file "$redis_tar_file" "http://download.redis.io/releases/$redis_tar_file"
        mv "$redis_file" "$REDIS_DIR"
        cd "redis"
        $MAKE
        cd ..
    fi
}

init_zk(){
    # Fetch Zookeeper
    if [[ ! -d $ZK_DIR ]]; then
        local zk_file="apache-zookeeper-$ZK_VERSION-bin"
        local zk_tar_file="$zk_file.tar.gz"
        fetch_untar_file "$zk_tar_file" "$APACHE_MIRROR/zookeeper/zookeeper-$ZK_VERSION/$zk_tar_file"
        mv "$zk_file" "$ZK_DIR"
    fi

    ## Set zookeeper configurations
    echo 'tickTime=2000' > $ZK_CONF_FILE
    echo "dataDir=/tmp/data/zk/" >> $ZK_CONF_FILE
    echo 'clientPort='$ZK_PORT >> $ZK_CONF_FILE
}

init_kafka(){
    # Fetch Kafka
    if [[ ! -d $KAFKA_DIR ]]; then
        local kafka_file="kafka_$SCALA_BIN_VERSION-$KAFKA_VERSION"
        local kafka_tar_file="$kafka_file.tgz"
        fetch_untar_file "$kafka_tar_file" "$APACHE_MIRROR/kafka/$KAFKA_VERSION/$kafka_tar_file"
        mv "$kafka_file" "$KAFKA_DIR"
    fi

    echo "dataDir=/tmp/data/zk" > $KAFKA_DIR/config/zookeeper.properties
    echo "clientPort=2181" >>     $KAFKA_DIR/config/zookeeper.properties

}

init_flink(){
    # Fetch Flink
    if [[ ! -d $FLINK_DIR ]]; then
        local flink_file="flink-$FLINK_VERSION"
        local flink_file_with_scala="$flink_file-bin-scala_${SCALA_BIN_VERSION}"
        local flink_tar_file="$flink_file_with_scala.tgz"
        fetch_untar_file "$flink_tar_file" "$APACHE_MIRROR/flink/$flink_file/$flink_tar_file"
        mv "$flink_file" $FLINK_DIR
    fi
}

init_flink_from_github(){
    # Remove old_target
     if [[ -d $FLINK_DIR ]]; then
        rm -r $FLINK_DIR
    fi

     # If Apache Flink is not built
     if [[ ! -d $FLINK_SRC_DIR ]]; then
        cd $HOME
        git clone -b release-1.8 https://github.com/apache/flink.git $FLINK_SRC_DIR
        maven_clean_install_no_tests $FLINK_SRC_DIR
     fi

     # Get Klink
     if [[ ! -d $KLINK_DIR ]]; then
        git clone https://github.com/oibfarhat/klink.git $KLINK_DIR
     fi

     # Move klink changes
     if [[ -d $FLINK_SRC_DIR/flink-runtime ]]; then
        rm -r $FLINK_SRC_DIR/flink-runtime
     fi

     if [[ -d $FLINK_SRC_DIR/flink-streaming-java ]]; then
        rm -r $FLINK_SRC_DIR/flink-streaming-java
     fi

     cp -r $KLINK_DIR/flink-runtime $FLINK_SRC_DIR/
     cp -r $KLINK_DIR/flink-streaming-java $FLINK_SRC_DIR/
     maven_clean_install_no_tests $FLINK_SRC_DIR/flink-runtime
     maven_clean_install_no_tests $FLINK_SRC_DIR/flink-streaming-java
     maven_clean_install_no_tests $FLINK_SRC_DIR/flink-dist
     mv $FLINK_SRC_DIR/build-target $FLINK_DIR

     echo 'metrics.latency.history-size: 65536' >> $FLINK_CONF_FILE
     echo 'metrics.latency.interval: 500' >> $FLINK_CONF_FILE

     echo 'metrics.scope.jm: <host>.jobmanager' >> $FLINK_CONF_FILE
     echo 'metrics.scope.jm.job: <host>.jobmanager.<job_name>' >> $FLINK_CONF_FILE
     echo 'metrics.scope.tm: <host>.taskmanager.<tm_id>' >> $FLINK_CONF_FILE
     echo 'metrics.scope.tm.job: <host>.taskmanager.<tm_id>.<job_name>' >> $FLINK_CONF_FILE
     echo 'metrics.scope.task: <host>.taskmanager.<tm_id>.<job_name>.<task_name>.<subtask_index>' >> $FLINK_CONF_FILE
     echo 'metrics.scope.operator: <host>.taskmanager.<tm_id>.<job_name>.<task_name>.<operator_name>.<subtask_index>' >> $FLINK_CONF_FILE
     echo 'metrics.scope.scheduler: <host>.<tm_id>.<scheduler_name>' >> $FLINK_CONF_FILE
}

setup(){
    init_setup_file

    if [[ ! -d "$BENCH_DIR" ]]; then
        mkdir "$BENCH_DIR"
    fi
    cd "$BENCH_DIR"

    init_redis
    init_zk
    init_kafka
    init_flink_from_github
}

setup

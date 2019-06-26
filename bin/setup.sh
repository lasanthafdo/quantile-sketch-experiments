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

# Flink download parameters
FLINK_VERSION=${FLINK_VERSION:-"1.8.0"}

# Zookeeper download parameters
ZK_VERSION=${ZK_VERSION:-"3.5.5"}
ZK_CONF_FILE="zoo.cfg"

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
    # Set zookeeper configurations
    cd "$ZK_DIR/conf"
    echo 'tickTime=2000' > $ZK_CONF_FILE
    echo "dataDir=$ZK_DIR/data" >> $ZK_CONF_FILE
    echo 'clientPort='$ZK_PORT >> $ZK_CONF_FILE
    cd $BENCH_DIR
}

init_kafka(){
    # Fetch Kafka
    if [[ ! -d $KAFKA_DIR ]]; then
        local kafka_file="kafka_$SCALA_BIN_VERSION-$KAFKA_VERSION"
        local kafka_tar_file="$kafka_file.tgz"
        fetch_untar_file "$kafka_tar_file" "$APACHE_MIRROR/kafka/$KAFKA_VERSION/$kafka_tar_file"
        mv "$kafka_file" "$KAFKA_DIR"
    fi
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
        git clone https://github.com/apache/klink.git $KLINK_DIR
     fi

     # Build and install changes
     maven_clean_install_no_tests $KLINK_DIR/flink-runtime
     maven_clean_install_no_tests $KLINK_DIR/flink-streaming-java
     maven_clean_install_no_tests $KLINK_DIR/flink-dist
     mv $KLINK_DIR/build-target $FLINK_DIR
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

cd $PROJECT_DIR
setup

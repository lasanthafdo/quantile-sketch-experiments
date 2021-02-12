#!/usr/bin/env bash

echo $0
bin=`dirname "$0"`
echo "$bin"
bin=`cd "$bin"; pwd`
echo "$bin"

. "$bin"/config.sh

# Apache Mirror link
APACHE_MIRROR="https://archive.apache.org/dist"

# Scala download parameters
SCALA_BIN_VERSION=${SCALA_BIN_VERSION:-"2.11"}

# Redis download parameters
REDIS_VERSION=${REDIS_VERSION:-"5.0.5"}

# Kafka download parameters
KAFKA_VERSION=${KAFKA_VERSION:-"2.4.1"}

init_setup_file(){
    # setup file
    echo 'kafka.brokers:' > $SETUP_FILE
    hname=$(hostname)

    if [[ $hname == "Harshs-MBP"  ]]; then
      echo '    - "localhost"' >> $SETUP_FILE
    else
      echo '    - "'tem101.tembo-domain.cs.uwaterloo.ca'"' >> $SETUP_FILE
    fi
    echo >> $SETUP_FILE
    echo 'zookeeper.servers:' >> $SETUP_FILE

    if [[ $hname == "Harshs-MBP"  ]]; then
      echo '    - "localhost"' >> $SETUP_FILE
    else
      echo '    - "'tem101.tembo-domain.cs.uwaterloo.ca'"' >> $SETUP_FILE
    fi
    echo >> $SETUP_FILE

    echo 'kafka.port: 9092' >> $SETUP_FILE
    echo 'zookeeper.port: '$ZK_PORT >> $SETUP_FILE
    if [[ $hname == "Harshs-MBP"  ]]; then
      echo 'redis.host: "localhost"' >> $SETUP_FILE
    else
      echo 'redis.host: "tem102.tembo-domain.cs.uwaterloo.ca"' >> $SETUP_FILE
    fi
    echo 'kafka.partitions: '1 >> $SETUP_FILE
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


init_kafka(){
    ## Fetch Kafka
    if [[ ! -d $KAFKA_DIR ]]; then
        local kafka_file="kafka_$SCALA_BIN_VERSION-$KAFKA_VERSION"
        local kafka_tar_file="$kafka_file.tgz"
        fetch_untar_file "$kafka_tar_file" "$APACHE_MIRROR/kafka/$KAFKA_VERSION/$kafka_tar_file"
        mv "$kafka_file" "$KAFKA_DIR"
    fi

    echo "dataDir=/tmp/data/zk" > $KAFKA_DIR/config/zookeeper.properties
    echo "clientPort=$ZK_PORT" >> $KAFKA_DIR/config/zookeeper.properties
}

init_flink(){
    # Fetch Flink

    # Remove old_target
     if [[ -d $FLINK_DIR ]]; then
        rm -r $FLINK_DIR
     fi

     if [[ $2 = 1 ]]; then
	echo "IF STATEMENT equal 1"
        if [[ -d $FLINK_SRC_DIR ]]; then
	    rm -r $FLINK_SRC_DIR
        fi
     fi

     # If Apache Flink is not built
     if [[ ! -d $FLINK_SRC_DIR ]]; then
        echo "Cloning Flink"
        git clone -b release-1.12 https://github.com/apache/flink.git $FLINK_SRC_DIR
        maven_clean_install_no_tests $FLINK_SRC_DIR
     fi

     cp -rf $FLINK_SRC_DIR/build-target $FLINK_DIR
}

init_synthetic_analytics(){
     # Remove old_target
     if [[ -d "$FLINK_DIR" ]]; then
        rm -r "$FLINK_DIR"
     fi

     # If Apache Flink is not built

     cp -r $HOME/flink-binary $FLINK_DIR

     echo "Printing META_INF/MANIFEST.MF file of $FLINK_SRC_DIR/lib to check java and flink version"
     firstfile=$FLINK_DIR/lib/$(ls -S $FLINK_DIR/lib | head -n 1)
     echo "$(unzip -p $firstfile META-INF/MANIFEST.MF)"
}

setup(){
    init_setup_file

    if [[ ! -d "$BENCHMARK_DIR" ]]; then
        mkdir "$BENCHMARK_DIR"
    fi
    cd "$BENCHMARK_DIR" || exit

    init_redis
    init_kafka

    if [[ $1 = "flink" ]]; then
        init_flink $1
    elif [[ $1 = "syn" ]]; then
        init_synthetic_analytics $1
    else
      echo "unrecogized option"
    fi
}

if [[ $# -lt 1 ]];
then
    echo "Invalid use: ./setup.sh MODE=[flink|syn]"
else
    cd "$PROJECT_DIR" || exit
    setup $1
fi

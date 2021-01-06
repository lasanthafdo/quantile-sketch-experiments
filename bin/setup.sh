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
#    if [[ $HAS_HOSTS -eq 1 ]]; then
#    echo '  -' >> $SETUP_FILE
#    while read line
#        do
#           echo "$line " >> SETUP_FILE
#           echo "server.$counter=$line:2888:3888" >> $ZK_CONF_FILE
#        done <${HOSTS_FILE}
#    fi
    echo '    - "'localhost'"' >> $SETUP_FILE
    echo >> $SETUP_FILE
    echo 'zookeeper.servers:' >> $SETUP_FILE
    echo '    - "localhost"' >> $SETUP_FILE
    echo >> $SETUP_FILE
    echo 'kafka.port: 9092' >> $SETUP_FILE
    echo 'zookeeper.port: '$ZK_PORT >> $SETUP_FILE
    echo 'redis.host: "localhost"' >> $SETUP_FILE
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
    echo "clientPort=$ZK_PORT" >>     $KAFKA_DIR/config/zookeeper.properties

    ## Check if in distributed mode
    if [[ $HAS_HOSTS -eq 1 ]]; then
        echo "Setting up Kafka multi-nodes configurations"
        # 1: Change Zookeeper.connect variable
        sed -i "s/zookeeper.connect=.*/zookeeper.connect=$ZK_CONNECTION/g" $KAFKA_DIR/config/server.properties
	    # 2: Change broker id for each node and move the file to /tmp/data/
	    local counter=0
	    local port=9092
	    while read line
	    do
            ((counter++))
            ssh_connect $line "
                cp $HOME/flink-benchmarks/kafka/config/server.properties /tmp/data/server.properties
                sed -i "s/broker.id=.*/broker.id=$counter/g" /tmp/data/server.properties
                echo "port=$port" >> /tmp/data/server.properties" 5
            ((port++))
        done <${HOSTS_FILE}
    fi
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
        git clone -b release-1.9 https://github.com/apache/flink.git $FLINK_SRC_DIR
        maven_clean_install_no_tests $FLINK_SRC_DIR
     fi

     cp -rf $FLINK_SRC_DIR/build-target $FLINK_DIR
}


init_watslack() {
     # Remove old_target
     if [[ -d $FLINK_DIR ]]; then
        rm -r $FLINK_DIR
     fi

     if [[ $2 = 1 ]]; then
        if [[ -d $FLINK_SRC_DIR ]]; then
	    rm -r $FLINK_SRC_DIR
        fi
     fi

     # If Apache Flink is not built
     if [[ ! -d $FLINK_SRC_DIR ]]; then
        #echo "Cloning Flink"
        #git clone https://github.com/ChasonPickles/streamingWithQuantification $FLINK_SRC_DIR
	mkdir $FLINK_SRC_DIR
	echo "Copying Flink/flink-streaming-java from Source to $FLINK_SRC_DIR..."
	cp -r $HOME/roughCopy/streamingWithQuantification/flink-streaming-java $FLINK_SRC_DIR
	sleep 2
	echo "Finished Copying"
        maven_install_no_tests $FLINK_SRC_DIR
        local firstfile=$FLINK_SRC_DIR/lib/$(ls -S $FLINK_SRC_DIR/lib | head -n 1)
        echo "$(unzip -p $firstfile META-INF/MANIFEST.MF)"
	sleep 2
     fi
     cp -rf $FLINK_SRC_DIR/build-target $FLINK_DIR
     local file=$FLINK_DIR/lib/$(ls -S $FLINK_DIR/lib | head -n 1)
     echo "$(unzip -p $file META-INF/MANIFEST.MF)"

     # Get Watslack
     #if [[ ! -d $WATSLACK_DIR ]]; then
     #	git clone https://github.com/ChasonPickles/streamingWithQuantification $WATSLACK_DIR
     #fi

     # Move flink changes
     # if [[ -d $FLINK_SRC_DIR/flink-runtime ]]; then
     #    sudo rm -r $FLINK_SRC_DIR/flink-runtime
     # fi

     #if [[ -d $FLINK_SRC_DIR/flink-streaming-java ]]; then
     #   sudo rm -r $FLINK_SRC_DIR/flink-streaming-java
     #fi

     # cp -r $WATSLACK_DIR/flink-runtime $FLINK_SRC_DIR/
     # cp -r $WATSLACK_DIR/flink-streaming-java $FLINK_SRC_DIR/
     # maven_clean_install_no_tests $FLINK_SRC_DIR/flink-runtime
     # echo "Installing flink-streaming-java"
     # maven_clean_install_no_tests $FLINK_SRC_DIR/flink-streaming-java
     # echo "Installing flink-dist"
     # maven_clean_install_no_tests $FLINK_SRC_DIR/flink-dist
     # mv $FLINK_SRC_DIR/build-target $FLINK_DIR

     sudo chmod -R 777 $PROJECT_DIR
}

init_synthetic_analytics(){
     # Remove old_target
     if [[ -d $FLINK_DIR ]]; then
        rm -r $FLINK_DIR
     fi

     if [[ $2 = 1 ]]; then
        if [[ -d $FLINK_SRC_DIR ]]; then
	    rm -r $FLINK_SRC_DIR
        fi
     fi

     # If Apache Flink is not built
     if [ ! -d $FLINK_SRC_DIR ] ; then
      #git clone -b release-1.12.0 https://github.com/ChasonPickles/flink.git $FLINK_SRC_DIR
      echo "Copying Flink from $HOME/flink to $FLINK_SRC_DIR..."

      mkdir $FLINK_SRC_DIR
      cp -r $HOME/flink/* $FLINK_SRC_DIR
      echo "Finished Copying"
      sleep 1
      cp -r $FLINK_SRC_DIR/build-target $FLINK_DIR
      #maven_install_no_tests $FLINK_SRC_DIR
     fi

     sleep 2


     echo "Printing META_INF/MANIFEST.MF file of $FLINK_SRC_DIR/lib to check java and flink version"
     firstfile=$FLINK_DIR/lib/$(ls -S $FLINK_DIR/lib | head -n 1)
     echo "$(unzip -p $firstfile META-INF/MANIFEST.MF)"
}

setup(){
    ## Create SETUP file first
    init_setup_file

    if [[ ! -d "$BENCHMARK_DIR" ]]; then
        mkdir "$BENCHMARK_DIR"
    fi
    cd "$BENCHMARK_DIR"

    init_redis
    init_kafka_zookeeper
    init_kafka

    if [[ $1 = "flink" ]]; then
        init_flink $1 $2
    elif [[ $1 = "watslack" ]]; then
        init_watslack $1 $2
    elif [[ $1 = "syn" ]]; then
        init_synthetic_analytics $1 $2
    fi
}

if [[ $# -lt 2 ]];
then
    echo "Invalid use: ./setup.sh MODE=[flink|watslack|syn] get_new{1=true, 0=false}"
else
    cd "$PROJECT_DIR"
    setup $1 $2
fi

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
SCALA_BIN_VERSION=${SCALA_BIN_VERSION:-"2.12"}

# Redis download parameters
REDIS_VERSION=${REDIS_VERSION:-"5.0.5"}

# Kafka download parameters
KAFKA_VERSION=${KAFKA_VERSION:-"2.2.0"}

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
    echo 'initLimit=5' >> $ZK_CONF_FILE
    echo 'syncLimit=2' >> $ZK_CONF_FILE

    ## Check if distributed mode
    if [[ $HAS_HOSTS -eq 1 ]]; then
        echo "Setting up ZK multi-nodes configurations"
        # 1: Input ZK servers
        local counter=0
        while read line
        do
           ((counter++))
           echo "server.$counter=$line:2888:3888" >> $ZK_CONF_FILE
        done <${HOSTS_FILE}

        # 2: Write myId in /tmp/ on all hosts
	    local counter=0
	    while read line
	    do
            ((counter++))
            ssh_connect $line "
                if [[ ! -d /tmp/data ]]; then
                    mkdir /tmp/data
                fi

                if [[ ! -d /tmp/data/zk ]]; then
                    mkdir /tmp/data/zk
                fi
                echo $counter > /tmp/data/zk/myid" 5
        done <${HOSTS_FILE}

        # 3: Fix ZK configs to point to loopback address and move the confs to /tmp/
        counter=0
        while read line
	    do
            ((counter++))
            ssh_connect $line "
                cp $ZK_CONF_FILE /tmp/data/zk/zoo.cfg
                sed -i 's/server.${counter}=.*/server.${counter}=0.0.0.0:2888:3888/g' /tmp/data/zk/zoo.cfg" 5
        done <${HOSTS_FILE}
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

     # If Apache Flink is not built
     if [[ ! -d $FLINK_SRC_DIR ]]; then
        echo "Cloning Flink"
        git clone -b release-1.9 https://github.com/apache/flink.git $FLINK_SRC_DIR
        maven_clean_install_no_tests $FLINK_SRC_DIR
     fi
}


init_watslack() {
    # Remove old_target
     if [[ -d $FLINK_DIR ]]; then
        rm -r $FLINK_DIR
    fi

     # If Apache Flink is not built
     if [[ ! -d $FLINK_SRC_DIR ]]; then
        echo "Cloning Flink"
        git clone https://github.com/ChasonPickles/streamingWithQuantification $FLINK_SRC_DIR
        # maven_clean_install_no_tests $FLINK_SRC_DIR
	maven_install_no_tests $FLINK_SRC_DIR
     fi

     # Get Watslack
     if [[ ! -d $WATSLACK_DIR ]]; then
	git clone https://github.com/ChasonPickles/streamingWithQuantification $WATSLACK_DIR
     fi

     # Move flink changes
     # if [[ -d $FLINK_SRC_DIR/flink-runtime ]]; then
     #    sudo rm -r $FLINK_SRC_DIR/flink-runtime
     # fi

     #if [[ -d $FLINK_SRC_DIR/flink-streaming-java ]]; then
     #   sudo rm -r $FLINK_SRC_DIR/flink-streaming-java
     #fi

     sudo chmod -R 777 $PROJECT_DIR

     # cp -r $WATSLACK_DIR/flink-runtime $FLINK_SRC_DIR/
     # cp -r $WATSLACK_DIR/flink-streaming-java $FLINK_SRC_DIR/
     # maven_clean_install_no_tests $FLINK_SRC_DIR/flink-runtime
     # echo "Installing flink-streaming-java"
     # maven_clean_install_no_tests $FLINK_SRC_DIR/flink-streaming-java
     # echo "Installing flink-dist"
     # maven_clean_install_no_tests $FLINK_SRC_DIR/flink-dist
     mv $FLINK_SRC_DIR/build-target $FLINK_DIR

     sudo chmod -R 777 $PROJECT_DIR
}

setup(){
    ## Create SETUP file first
    init_setup_file

    ## Create $BENCH_DIR
    if [[ ! -d "$BENCH_DIR" ]]; then
        mkdir "$BENCH_DIR"
    fi
    cd "$BENCH_DIR"

    ## Install Redis
    # init_redis
    ## Install ZooKeeper
    # init_zk
    ## Install Kafka
    # init_kafka

    # Install Flink
    if [[ $1 = "flink" ]]; then
        init_flink
    # Install watslack
    elif [[ $1 = "watslack" ]]; then
        init_watslack
    fi
}

if [[ $# -lt 1 ]];
then
    echo "Invalid use: ./setup.sh MODE=[flink|watslack]"
else
    cd "$PROJECT_DIR"
    setup $1
fi

#!/bin/bash

case $1 in
"start"){
    for i in rtgMaster rtgSlave1 rtgSlave2
    do
        echo " --------start $i Kafka-------"
        ssh $i "/home/xuan.liu/software/kafka/bin/kafka-server-start.sh -daemon /home/xuan.liu/software/kafka/config/server.properties "
    done
};;
"stop"){
    for i in rtgMaster rtgSlave1 rtgSlave2
    do
        echo " --------stop $i Kafka-------"
        ssh $i "/home/xuan.liu/software/kafka/bin/kafka-server-stop.sh stop"
    done
};;
esac

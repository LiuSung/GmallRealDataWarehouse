#!/bin/bash

case $1 in
"start"){
        for i in rtgMaster rtgSlave1
        do
                echo " --------启动 $i 采集flume-------"
                ssh $i "nohup /home/xuan.liu/software/flume/bin/flume-ng agent -n a1 -c /home/xuan.liu/software/flume/conf/ -f /home/xuan.liu/software/flume/job/file_to_kafka.conf >/dev/null 2>&1 &"
        done
};;
"stop"){
        for i in rtgMaster rtgSlave1
        do
                echo " --------停止 $i 采集flume-------"
                ssh $i "ps -ef | grep file_to_kafka | grep -v grep |awk  '{print \$2}' | xargs -n1 kill -9 "
        done

};;
esac

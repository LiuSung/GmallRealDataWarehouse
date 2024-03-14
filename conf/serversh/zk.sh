#!/bin/bash

case $1 in
"start"){
	for i in rtgMaster rtgSlave1 rtgSlave2
	do
        echo ---------- zookeeper $i start ------------
		ssh $i "/home/xuan.liu/software/zookeeper/bin/zkServer.sh start"
	done
};;
"stop"){
	for i in rtgMaster rtgSlave1 rtgSlave2
	do
        echo ---------- zookeeper $i stop  ------------    
		ssh $i "/home/xuan.liu/software/zookeeper/bin/zkServer.sh stop"
	done
};;
"status"){
	for i in rtgMaster rtgSlave1 rtgSlave2
	do
        echo ---------- zookeeper $i status ------------    
		ssh $i "/home/xuan.liu/software/zookeeper/bin/zkServer.sh status"
	done
};;
esac

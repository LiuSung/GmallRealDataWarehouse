### ODS-To-DIM数据处理

#### DIM层存储

​	DIM 层表是用于维度关联的，要通过主键去获取相关维度信息，这种场景下 K-V 类型数据库的效率较高。常见的 K-V 类型数据库有 Redis、HBase，而 Redis 的数据常驻内存，会给内存造成较大压力，因而选用 HBase 存储维度数据。

​	搭建HBase+phoneix

![image-20240111205655409](https://raw.githubusercontent.com/LiuSung/Images/main/img/202401132241272.png)

#### 需求分析

​	ODS-To-DIM：从Kafka中获取mysql中的46张表的业务数据，并从中过滤出维度表的insert、update、bootstrap-instert数据，并将数据写入到HBase，并做到当新增维度表或者修改维度表字段时不修改代码，不重启程序便可以新建或更新HBase中维度表。

​	思路：使用flinkCDC监控一张表tableprocess(字段包含SourceTable、SinkTable、TableColumns、TablePrimaryKey、TableExtend)，Sourcetable为维度表表名、SinkTable为写入phoneix表表名(与SourceTable一致)、TableColumns为表的字段信息、TablePrimaryKey是主键信息、TableExtend为扩展字段。将tableprocess（该表的作用是当新建维度表时，我们只需要维护该表的内容就可实现HBase中维度表的自动创建，并且当mysql维度表新增数据和更新时自动插入和更新HBase中维度表数据）获取的数据流做成广播流，将主流和广播流connect，之后调用process算子。

**广播流：Tableprocess表的数据**

```
{"database":"gmall","table":"favor_info","type":"insert","ts":1704798219,"xid":217972,"commit":true, "data":{"id":1744269580549595177,"user_id":null,"sku_id":null,"spu_id":null,"is_cancel":null,"create_time":null,"cancel_time":null}}
```

TODO

	1. 格式化数据
	1. 拼接建表语句并在HBase中创建维度表
	1. 写入状态（SinkTable，TableProcess）广播出去

**主流：kafka中获取的ods_topic_db业务数据库信息**

```
{"before":null,"after"{"source_table":"aa","sink_table":"bb","sink_columns":"cc","sink_pk":"dd","sink_extend":"ee"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1704894465000,"snapshot":"false","db":"gmall-config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000008","pos":377,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1704894465683,"transaction":null}

```

TODO

1. 获取并解析广播流数据
2. 主流中数据过滤(只保留TableColumns字段和数据)
3. 主流数据put SinkTable然后写collect写出

#### ODS-To-DIM数据处理可视化

![image-20240111215429100](https://raw.githubusercontent.com/LiuSung/Images/main/img/202401132241316.png)

#### 执行任务（flink on yarn）

```
bin/flink run -d -t yarn-per-job -c com.gmall.realtime.app.dim.DimApp flink-gmall-1.0-SNAPSHOT.jar
```

![image-20240313154547566](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403131545676.png)

初始化tableprocess表将维表数据写入该表，如下图维表插入HBase中

![image-20240313154745657](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403131548939.png)

![image-20240313155744783](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403131557640.png)

![image-20240313160102553](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403131601137.png)

#### 造数据查看数据是否写入phoenix表中

![image-20240313170007891](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403131700527.png)

![image-20240313165937865](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403131659409.png)

#### 遇到问题

1. 实验环境为docker 环境，在docker commit 时会有多个版本的镜像，因为在打包镜像时zk和Hbase有多次start和stop导致zk保存的Hbase元数据与Hbase本身不一致。解决：因为还没有对维度数据进行初始化，所以在zkCli中直接rmr /hbase
2. phoenix-5.0.0-HBase-2.0-client.jar 冲突问题：phoenix与打的jar包中phoenix-5.0.0-HBase-2.0-client.jar冲突，报错为Caused by: java.lang.ClassNotFoundException: org.apache.hadoop.mapreduce.InputFormat。解决：在pom文件中phoenix-5.0.0-HBase-2.0-client.jar位置`<scope>provided</scope>`掉，然后把phoenix-5.0.0-HBase-2.0-client.jar copy到 flink/lib下。
3. Flink写入phoenix中的数据中文乱码：flink-con.yaml中添加：env.java.opts: -Dfile.encoding=UTF-8
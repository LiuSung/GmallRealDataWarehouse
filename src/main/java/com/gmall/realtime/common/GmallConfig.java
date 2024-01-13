package com.gmall.realtime.common;

public class GmallConfig {
    // Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:192.168.141.100,192.168.141.101,192.168.141.102:2181";

    // Kafka就能
    public static final String KAFKA_SERVER = "192.168.141.100:9092,192.168.141.101:9092,192.168.141.102:9092";
}

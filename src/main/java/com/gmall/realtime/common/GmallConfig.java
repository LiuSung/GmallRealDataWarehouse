package com.gmall.realtime.common;

public class GmallConfig {
    // Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:172.19.133.10,172.19.133.11,172.19.133.12:2181";

    // Kafka就能
    public static final String KAFKA_SERVER = "172.19.133.10:9092,172.19.133.11:9092,172.19.133.12:9092";

    // ClickHouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // ClickHouse 连接 URL
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://172.19.133.12:8123/gmall_rebuild";
}

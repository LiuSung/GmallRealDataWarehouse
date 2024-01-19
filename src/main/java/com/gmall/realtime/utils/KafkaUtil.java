package com.gmall.realtime.utils;


import com.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;


public class KafkaUtil {
    public static String kafkaServer = GmallConfig.KAFKA_SERVER;
    public static KafkaSource<String> getKafkaSource(String topic,String groupid){
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaServer)
                .setTopics(topic)
                .setGroupId(groupid)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new DeserializationSchema() {
                    @Override
                    public TypeInformation getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }

                    @Override
                    public Object deserialize(byte[] bytes) throws IOException {
                        if (bytes != null || bytes.length >0){
                            return new String(bytes);
                        }else {
                            return null;
                        }

                    }

                    @Override
                    public boolean isEndOfStream(Object o) {
                        return false;
                    }
                }).build();
        return kafkaSource;
    }

    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic){
        return new FlinkKafkaProducer<>(kafkaServer,topic,new SimpleStringSchema());
    }

    public static String getKafkaDDL(String topic,String groupid){
        return "WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '"+topic+"'," +
                "  'properties.bootstrap.servers' = '"+GmallConfig.KAFKA_SERVER+"'," +
                "  'properties.group.id' = '"+groupid+"'," +
                "  'scan.startup.mode' = 'group-offsets'," +
                "  'format' = 'json'" +
                ")";
    }

    public static String getKafkaSinkDDL(String topic){
        return "WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '"+topic+"'," +
                "  'properties.bootstrap.servers' = '"+GmallConfig.KAFKA_SERVER+"'," +
                "  'scan.startup.mode' = 'group-offsets'," +
                "  'format' = 'json'" +
                ")";
    }
    public static String getKafkaUpsertSinkDDL(String topic){
        return "WITH (" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = '"+topic+"'," +
                "  'properties.bootstrap.servers' = '"+GmallConfig.KAFKA_SERVER+"'," +
                "  'key.format' = 'json'," +
                "  'value.format' = 'json'" +
                ")";
    }

    /**
     * topic_db 主题数据的Kafka-Source DDL语句
     * @param groupid 消费者组
     * @return
     */
    public static String getTopicDb(String groupid){
        return "CREATE TABLE `topic_db` (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `data` MAP<STRING,STRING>,\n" +
                "  `old` MAP<STRING,STRING>,\n" +
                "  `pt` AS PROCTIME()\n" +
                ")" + getKafkaDDL("topic_db",groupid);
    }

}

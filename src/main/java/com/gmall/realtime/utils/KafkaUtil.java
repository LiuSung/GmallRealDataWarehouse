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
}

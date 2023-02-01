package org.example.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.example.common.GmallConfig;

import java.util.Properties;

public class MyKafkaUtil {

    public static KafkaSink<String> getKafkaProducer(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(GmallConfig.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
    }

    /**
     * todo flink 1.14 kafka 动态sink到不同的topic中
     *
     * @return
     */
    public static <T> KafkaSink<T> getKafkaSinkProducer(KafkaRecordSerializationSchema<T> kafkaSerializationSchema) {

        return KafkaSink.<T>builder()
                .setBootstrapServers(GmallConfig.KAFKA_BROKERS)
                .setRecordSerializer(kafkaSerializationSchema)
                .build();
    }

    /**
     * 动态sink到不同的topic中
     *
     * @param kafkaSerializationSchema
     * @param <T>
     * @return
     */
    @Deprecated
    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, GmallConfig.KAFKA_BROKERS);

        return new FlinkKafkaProducer<T>(GmallConfig.KAFKA_DEFAULT_TOPIC, kafkaSerializationSchema, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

    }

    public static KafkaSource<String> getKafkaConsumer(String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(GmallConfig.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    public static String getKafkaDDL(String topic, String groupId) {

        return " 'connector' = 'kafka'," +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + GmallConfig.KAFKA_BROKERS + "'," +
                " 'properties.group.id' = '" + groupId + "'," +
                " 'scan.startup.mode' = 'earliest-offset'," +
                " 'format' = 'json' ";


    }


}

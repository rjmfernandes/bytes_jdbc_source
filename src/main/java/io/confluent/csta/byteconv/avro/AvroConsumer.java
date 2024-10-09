package io.confluent.csta.byteconv.avro;

import io.confluent.csta.byteconv.MyTable;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;



public class AvroConsumer {

    public static void main(String[] args) throws UnsupportedEncodingException {
        String topicName = "postgres2-mytable2";
        String bootstrapServers = "localhost:9092";
        String schemaRegistryUrl = "http://localhost:8081";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "specific-avro-consumer-group"+System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("specific.avro.reader", "true");
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, MyTable> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        while (true) {
            ConsumerRecords<String, MyTable> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, MyTable> record : records) {
                SpecificRecordBase avroRecord = record.value();
                System.out.println("Key: " + record.key());
                System.out.println("Value: " + avroRecord);
                System.out.println("Decoded data: " + new String(
                        ((MyTable)avroRecord).getData().array(),
                        "IBM285"));
            }
        }
    }
}
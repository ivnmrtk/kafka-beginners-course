package com.github.ivnmrtk.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        //creating producer propetries
        Properties properties = new Properties();

        String bootstrapServers = "localhost:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        String serializerName = StringSerializer.class.getName();

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializerName);

        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerName);

        //create producer

        KafkaProducer producer = new KafkaProducer<String, String>(properties);

        //create producer record

        ProducerRecord record = new ProducerRecord<String, String>("first_topic", "hello world");

        //send data (async)
        producer.send(record);

        producer.flush();
        producer.close();
    }

}

package com.github.ivnmrtk.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
        //creating producer propetries
        Properties properties = new Properties();

        String bootstrapServers = "localhost:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        String serializerName = StringSerializer.class.getName();

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializerName);

        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerName);

        //create producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        for (int i = 0; i < 10; i++) {

            //create producer record

            String topic = "first_topic";
            String value = "hello world " + i;
            String key = "id_" + i;

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            logger.info("Key" + key);

            //send data (async)
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("Received new metadata. \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp() + "\n");
                } else {
                    logger.error("Error while producing", exception);
                }
            }).get(); //bad practice - make send synchronous
        }

        producer.flush();
        producer.close();
    }

}

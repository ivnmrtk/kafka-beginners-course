package com.github.ivnmrtk.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        //creating producer propetries
        Properties properties = new Properties();

        String bootstrapServers = "localhost:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        String serializerName = StringSerializer.class.getName();

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializerName);

        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerName);

        //create producer

        KafkaProducer producer = new KafkaProducer<String, String>(properties);


        for (int i = 0; i < 10; i++) {

            //create producer record

            ProducerRecord record = new ProducerRecord<String, String>("first_topic", "hello world " + i);

            //send data (async)
            producer.send(record, (metadata,exception )  -> {
                    if (exception == null) {
                        logger.info("Received new metadata. \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp() + "\n");
                    } else {
                        logger.error("Error while producing", exception);
                    }
                });
        }

        producer.flush();
        producer.close();
    }

}

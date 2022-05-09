package com.saraya.tuto1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
        String bootstrap = "127.0.0.1:9092";

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Create Producer Record
        for (int i = 0; i <= 10; i++) {
            String topic = "laila3";
            String value = "Hello World " + Integer.toString(i);
            String key = "id_ " + Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            log.info("Key " + key);


            //Send Data  - Asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        log.info("Received Metadata. \n " +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "OffSet: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());

                    } else {
                        log.error("An Error Occurred", e);
                    }
                }
            }).get();  //block the .send() to make synchronous but never do this in production
        }
            producer.flush();
            producer.close();
        }
        //The command that I use on the CLI of KAFKA through Docker
        // kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic laila3  --group my-first-application
    }


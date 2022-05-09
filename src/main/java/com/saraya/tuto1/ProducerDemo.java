package com.saraya.tuto1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        String bootstrap =  "127.0.0.1:9092";

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrap);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String , String> producer = new KafkaProducer<String, String>(properties);

        //Create Producer Record
        ProducerRecord<String , String> record = new ProducerRecord<>("laila3" , "Hello World");


        //Send Data  - Asynchronous
        producer.send(record);

        producer.flush();
        producer.close();
    }
    //The command that I use on the CLI of KAFKA through Docker
    // kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic laila3  --group my-first-application
}

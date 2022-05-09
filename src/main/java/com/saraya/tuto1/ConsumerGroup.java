package com.saraya.tuto1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerGroup {
    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(ConsumerGroup.class.getName());
        String bootstrap = "127.0.0.1:9092";
        String groupId = "My-second-application";
        String topic = "laila3";

        //Create  Consumer Configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrap);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG , groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest");


        //Create Consumer
        KafkaConsumer<String , String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe Consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));


        //Poll for new Data
        while(true){
            ConsumerRecords<String , String> records =
                    consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records){
                log.info("Key " + record.key() + ", Value " + record.value() +
                        ", Offset " + record.offset() + ", Partition " + record.partition());
            }
        }

    }
}

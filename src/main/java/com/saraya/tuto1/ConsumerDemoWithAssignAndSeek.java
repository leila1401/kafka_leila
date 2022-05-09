package com.saraya.tuto1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithAssignAndSeek {
    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(ConsumerDemoWithAssignAndSeek.class.getName());
        String bootstrap = "127.0.0.1:9092";
      //  String groupId = "My-first-application";
        String topic = "laila3";

        //Create  Consumer Configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrap);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG , groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest");


        //Create Consumer
        KafkaConsumer<String , String> consumer = new KafkaConsumer<String, String>(properties);

       //Assign and Seek are mostly use to replay data or fetch a specific message

        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic , 0);
        Long offSetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom , offSetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;


        //Poll for new Data
        while(keepOnReading){
            ConsumerRecords<String , String> records =
                    consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records){
                numberOfMessagesReadSoFar += 1;
                log.info("Key " + record.key() + ", Value " + record.value() +
                        ", Offset " + record.offset() + ", Partition " + record.partition());
                if(numberOfMessagesReadSoFar >=  numberOfMessagesToRead){
                    keepOnReading = false; //to exit the while loop
                    break;  //to exit to for loop
                }
            }
        }
        log.info("Exiting the application");

    }
}

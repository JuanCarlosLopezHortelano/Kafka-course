package com.prueba.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {


    private static final Logger log  = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {


        log.info("I am Consumer");

        //create Producer Properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "172.24.229.184:9092");

        String groupId = "my-java-application";
        String topic = "topico_test";


        // creaste consumer configs

        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        props.setProperty("group.id", groupId);
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        //create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);

        //get a reference to a main thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook+
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detecting Shutting down, lets exit by calling consumer.wakeup() ");
                consumer.wakeup();


                //join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        try{
        //subscribete to a topic
        consumer.subscribe(Arrays.asList(topic));

        //pal for data
        while (true) {

         ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));

         for (ConsumerRecord<String,String> record : records) {
             log.info("Key: " + record.key() + ", Value: " + record.value());
             log.info("Partition: " + record.partition() + ", Offset: " + record.offset());

         }
        }}
        catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.error("Error while consuming", e);

        }
        finally {
            consumer.close(); //close the consumer, this also commit oofset
            log.info("Shutting down");
        }
    }
}

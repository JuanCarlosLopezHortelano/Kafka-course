package com.prueba.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {


    private static final Logger log  = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am kafka producer");

        //create Producer Properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "172.24.229.184:9092");



        //set kafka producer properties
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

          props.setProperty("batch.size", "400");



        //create the Producer
            KafkaProducer<String,String> producer = new KafkaProducer<>(props);

        //create a Producer Record



        for (int j = 0; j < 10; j++) {

        for (int i = 0; i < 30; i++) {

            ProducerRecord<String,String> producerRecord =
                    new ProducerRecord<>("topico_test", "Hello Kafka" + i);
            //send data
            producer.send(producerRecord,new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {log.info("Successfully sent data \n"+
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "TimeStamp: " + metadata.timestamp() + "\n" );
                    }
                    else {
                        log.error("Error while sending message", exception);
                    }
                }
            });
        }

            try{
                Thread.sleep(500);

            }catch(InterruptedException e){
                e.printStackTrace();
            }

        }


        //flush and chose the producer

        //tell the producer to send all data and block until done --synchronous operation
        producer.flush();

        // flush and close the producer
        producer.close();

    }
}

package com.prueba.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {


    private static final Logger log  = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello Kafka");

        //create Producer Properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");



        //set kafka producer properties
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());


        //create the Producer
            KafkaProducer<String,String> producer = new KafkaProducer<>(props);

        //create a Producer Record

        ProducerRecord<String,String> producerRecord =
                new ProducerRecord<>("first--topic", "Hello Kafka");

        //send data
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("✅ Mensaje enviado:");
                System.out.println("  Tópico: " + metadata.topic());
                System.out.println("  Partición: " + metadata.partition());
                System.out.println("  Offset: " + metadata.offset());
            } else {
                System.err.println("❌ Error al enviar:");
                exception.printStackTrace();
            }
        });


        //flush and chose the producer

        //tell the producer to send all data and block until done --synchronous operation
        producer.flush();

        // flush and close the producer
        producer.close();

    }
}

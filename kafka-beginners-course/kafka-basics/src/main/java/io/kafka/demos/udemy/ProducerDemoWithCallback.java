package io.kafka.demos.udemy;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        // create Producer Properties
        Properties properties = new Properties();

        // connect to Localhost
        properties.setProperty("bootstrap.servers", "[::1]:9092");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

       // force smaller batch size just to demonstrate, never used in production, keep Kafka default 16kB
        properties.setProperty("batch.size", "400");

        // force behavior of going to every different partitions
        // not recommended in production
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //  UniformPartitioner with Sticky Partitioner
//        for (int i=0; i<10; i++){
//            // create a Producer Record
//            ProducerRecord<String, String> producerRecord =
//                    new ProducerRecord<>("demo_java", "Hello World!" + i);
//
//            // send data
//            producer.send(producerRecord, new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata metadata, Exception e) {
//                    // executes every time a record successfully sent or and exception is thrown
//                    if(e ==  null){
//                        // the record was successfully sent
//                        log.info("Receive new metadata \n" +
//                                "Topic: " + metadata.topic() + "\n" +
//                                "Partition: " + metadata.partition() + "\n" +
//                                "Offset: " + metadata.offset() + "\n" +
//                                "Timestamp: " + metadata.timestamp());
//                    }
//                    else {
//                        log.error("Error while producing", e);
//                    }
//                }
//            });
//        }

        // create more messages and wait to change UniformPartitioner behavior
        for (int j = 0;  j<10; j++){
            for (int i = 0; i<30; i++) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("demo_java", "Hello World!" + i);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e == null) {
                            log.info("Receive new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}

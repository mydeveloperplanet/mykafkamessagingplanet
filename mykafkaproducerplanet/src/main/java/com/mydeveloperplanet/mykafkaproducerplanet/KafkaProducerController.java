package com.mydeveloperplanet.mykafkaproducerplanet;

import org.apache.kafka.clients.producer.*;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

@RestController
public class KafkaProducerController {

    private int counter;

    @RequestMapping("/sendMessages/")
    public String sendMessages() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("my-kafka-topic", Integer.toString(counter), Integer.toString(counter)),

                    (metadata, e) -> {
                        if(e != null) {
                            e.printStackTrace();
                        } else {
                            System.out.println("The offset of the record we just sent is: " + metadata.offset());
                        }
                    }
            );
            counter++;
        }

        producer.close();

        return "Messages sent";

    }

}

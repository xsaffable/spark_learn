package com.gjxx.java.kafka.learn;

import lombok.Cleanup;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author Admin
 */
@Log4j2
public class KafkaUtil {

    /**
     * producer
     */
    public static void send() {
        Properties props = new Properties();
        try {
            props.load(ClassLoader.getSystemResourceAsStream("dev/kafka_producer.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        //序列化器
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        @Cleanup
        Producer<String, String> producer = new KafkaProducer<>(props);

        int i = 0;
        while (true) {
            producer.send(new ProducerRecord<>("test", Integer.toString(i), "dd:"+i));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            i++;
            log.info("log dd:"+i);
        }

    }



}

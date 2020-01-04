package com.example.chapter4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import jdk.nashorn.internal.runtime.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.util.VisibleForTesting;
import org.junit.Test;

public class HozTest {

    @Test
    public void gettingStartedProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        ArrayList<String> integers = new ArrayList<>();
        integers.add("hoge");
        integers.add("huga");

        try (KafkaProducer<Integer, String> producer =
                     new KafkaProducer<>(properties, new IntegerSerializer(), new StringSerializer())) {
                    IntStream
                            .rangeClosed(1, 10)
                            .forEach(i -> {
                                try {
                                    producer.send(new ProducerRecord<>("my-topic", i, "value" + i)).get();
                                } catch (InterruptedException | ExecutionException e) {
                                    throw new RuntimeException(e);
                                }
                            });
        }
    }

    @Test
    public void gettingStartedConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        KafkaConsumer<Integer, String> consumer =
                new KafkaConsumer<>(properties, new IntegerDeserializer(), new StringDeserializer());

        List<ConsumerRecord<Integer, String>> received = new ArrayList<>();

        consumer.subscribe(Arrays.asList("my-topic"));


        //print each record.
        received.forEach(record -> {
            System.out.println("Record Key " + record.key());
            System.out.println("Record value " + record.value());
            System.out.println("Record partition " + record.partition());
            System.out.println("Record offset " + record.offset());
        });    }
}

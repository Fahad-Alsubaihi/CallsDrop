package org.example;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class DropCallsConsumer implements Runnable{
    private Thread thread;
    private String topic;
    private int partitionnum;
    private long offset;
//    String topic = "";
public DropCallsConsumer(String topic,int partition,long offset){
    this.topic=topic;
    this.partitionnum=partition;
    this.offset=offset;
}


    @Override
    public void run() {


        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"group2");


        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url","http://localhost:8081");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);
//    assign
        TopicPartition partition = new TopicPartition(topic,partitionnum);
//    consumer.subscribe(Arrays.asList(topic));
        consumer.assign(Arrays.asList(partition));
//    seek
        consumer.seek(partition, offset);

        try

        {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
                }
            }
        } finally

        {
//            consumer.close();
        }
    }
    public void start () {
        System.out.println("Starting Consumer");
        if (thread == null) {
            thread = new Thread (this);
            thread.start ();
        }
    }
}

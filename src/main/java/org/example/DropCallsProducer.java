package org.example;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.Date;
import java.util.Properties;
import java.util.Properties;
import java.util.UUID;

import static java.lang.Thread.sleep;

public class DropCallsProducer implements Runnable{
    private Thread thread;
    private String topic;
    private String key;
    public DropCallsProducer(String topic,String key) {
this.topic=topic;
this.key=key;

//        producer.close();



    }

    @Override
    public void run() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("max.block.ms", 30000);
        props.put("schema.registry.url", "http://localhost:8081");
        KafkaProducer producer = new KafkaProducer(props);



        String userSchema = "{\"type\":\"record\"," +
                "\"name\":\"myrecord\"," +
                "\"fields\":[{\"name\":\"phone\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"position\",\"type\":\"string\"},{\"name\":\"DTTM\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        GenericRecord avroRecord = new GenericData.Record(schema);

People_list pl=new People_list();



        while (true) {
//            Date date =new Date();
            People people=pl.getrand();
            avroRecord.put("phone", people.phone);
            avroRecord.put("name", people.name);
            avroRecord.put("position", people.position);
            avroRecord.put("DTTM", System.currentTimeMillis());
//            avroRecord.put("DTTM", new java.sql.Date(System.currentTimeMillis()).toLocalDate().toEpochDay());
            ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, key, avroRecord);

            try {
                producer.send(record);
                Thread.sleep(3000);
            } catch (SerializationException e) {
                // may need to do something with it
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
// When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
// then close the producer to free its resources.
//            finally {
//                producer.flush();

//                producer.close();
//            }
//            System.out.println("id ="+id);
        }
    }
    public void start () {
        System.out.println("Starting Producer");
        if (thread == null) {
            thread = new Thread (this);
            thread.start ();
        }
    }
}



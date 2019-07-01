package myapps;


import com.opencsv.CSVReaderHeaderAware;
import odg.test.avro.Passenger;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;


import static java.util.stream.Collectors.toList;


public class PassengerDriver {

    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "broker:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://schema-registry:8081";
        produceInputs(bootstrapServers, schemaRegistryUrl);

    }




    private static void produceInputs(final String bootstrapServers, final String schemaRegistryUrl) {


        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        final KafkaProducer<String, Passenger> producer = new KafkaProducer<>(props);

        CSVReaderHeaderAware reader;
        try {
            reader = new CSVReaderHeaderAware(new FileReader("/data/train.csv"));
        }
        catch(Exception e) {
            System.out.println(e);
            return;
        }

        Map<String, String> record;

        Passenger passenger;
        try {
            while ((record = reader.readMap()) != null) {
                System.out.println(record);


                try {
                passenger = Passenger.newBuilder().setId(Integer.valueOf(record.get("PassengerId")))
                        .setPclass(Integer.valueOf(record.get("Pclass")))
                        .setSex(record.get("Sex"))
                        .setSurvived( Integer.valueOf(record.get("Survived")))
                        .setEmbarked(record.get("Embarked"))
                        .setSibsp(Integer.valueOf(record.get("SibSp")))
                        .setAge(Integer.valueOf(record.get("Age")))
                        .setFare( Float.valueOf(record.get("Fare")))
                        .build();

                }
                catch(Exception exc) {
                    System.out.println(exc);
                    continue;
                }

                producer.send(new ProducerRecord<>("passenger", null, passenger));
            }

        }

        catch(Exception e) {
            System.out.println(e);
        return;
        }





        producer.flush();

    }






}

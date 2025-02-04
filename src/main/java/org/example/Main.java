package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.example.models.Coordinates;
import org.example.serdes.CoordinatesDeserializer;
import org.example.serdes.CoordinatesSerializer;
import org.example.serdes.SerdeFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class Main {

    public static void main(String[] args) {

        System.out.println("Hello, World!");

        final Properties props = new Properties();

        final String LOC_STREAM = "locations";


        KafkaStreams streams = null;

        // https://kafka.apache.org/23/documentation/streams/developer-guide/config-streams.html
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // SerDes for key and value
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,     Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,   Serdes.String().getClass().getName());


        final StreamsBuilder sbuilder = new StreamsBuilder();

        KStream<String, String> streamWords = sbuilder.stream("words");
        KStream<String, String> streamSent  = sbuilder.stream("sentences");

        // Produce some locations
        final Producer<String, Coordinates> locationProducer = getLocationProducer();
        for(int i=0; i<100000; i+=100) {
            var c = new Coordinates( 61.499+(Math.sqrt(i)), 23.777+(Math.sqrt(i)) );
            sendCoordinates(locationProducer, LOC_STREAM, c);
        }

        // Stream of locations (coordinates)
        KStream<String, Coordinates> streamLocs = sbuilder.stream(LOC_STREAM, Consumed.with(Serdes.String(), SerdeFactory.getCoordinatesSerde()));

        streamLocs.foreach( (k,v) -> {
            System.out.printf("%s @ %s%n", k, v);
        });

        streamSent.flatMapValues( val -> Arrays.asList(val.split("\\s")) ).to("words");    // whitespace split


        KTable<String, Long> wg = streamWords
                .groupBy( (k, v) -> v )
                .count(Materialized.as("word-count"));

        KTable<String, Long> filteredWg = wg.filter((k, v) -> v >= 3);

        // Print out common words
        filteredWg.toStream().foreach((key, value)
                -> System.out.printf("Common words: %s, '%d times'%n", key, value)
        );

        streamWords
            .filter( (k,v) -> v != null )
            .mapValues(value -> value.toUpperCase()
        ).foreach((key, value) -> {
            System.out.printf("Key: %s => '%s'%n", key, value);
        });



        try {
            streams = new KafkaStreams(sbuilder.build(), props);
            streams.start();
        } catch (Exception e) {
            System.out.println(e);
        }

        KafkaStreams finalStreams = streams;
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                finalStreams.close();
            }
        });

    }

    private static KafkaProducer<String, Coordinates> getLocationProducer() {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.example.serdes.CoordinatesSerializer");
        return new KafkaProducer(props);
    }

    /*
    * Sends location to given topic that is type <String, Coordinates>
    * */
    private static void sendCoordinates(Producer<String, Coordinates> p, String topic, Coordinates c) {
        final String key = UUID.randomUUID().toString();
        ProducerRecord<String, Coordinates> record = new ProducerRecord<>(topic, key, c);
        p.send(record);
    }

    // Writes topology to stream
    private static void describe(StreamsBuilder b) {
        if(b != null) {
            final Topology top = b.build();
            System.out.println( top.describe() );
        }
    }

}
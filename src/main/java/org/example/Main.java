package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {

        System.out.println("Hello, World!");

        final Properties props = new Properties();
        KafkaStreams streams = null;

        // https://kafka.apache.org/23/documentation/streams/developer-guide/config-streams.html
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // SerDes for key and value
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,     Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,   Serdes.String().getClass().getName());

        final StreamsBuilder sbuilder = new StreamsBuilder();
//        KStream<String, String> stream      = sbuilder.stream("plaintext-input");

        KStream<String, String> streamWords = sbuilder.stream("words");
        KStream<String, String> streamSent  = sbuilder.stream("sentences");
        streamSent.flatMapValues( val -> Arrays.asList(val.split("\\s")) ).to("words");    // whitespace split


        final Topology top = sbuilder.build();
        // Writes topology to stream
        // System.out.println( top.describe() );

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
}
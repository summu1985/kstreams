package com.redhat.demo.kstreams;

import java.util.Arrays;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class WordCountProcessor {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder
          .stream("streams-plaintext-input", Consumed.with(STRING_SERDE, STRING_SERDE));

        // KTable<String, Long> wordCountsOutput = messageStream
        // .flatMapValues(value -> Arrays.asList(value.toLowerCase().split(" ")))
        // .groupBy((key, value) -> value)
        // .count();

        KTable<String, Long> wordCounts = messageStream
        .flatMapValues(value -> Arrays.asList(value.toLowerCase().split(" ")))
        .groupBy((key, value) -> value)
        .count(Materialized.as("counts"));

        //wordCounts.toStream().to("output-topic");
         //wordCountsOutput.toStream().print(Printed.toSysOut());
    }
}
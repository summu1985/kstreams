package com.redhat.demo.kstreams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import lombok.AllArgsConstructor;

import org.springframework.kafka.config.StreamsBuilderFactoryBean;

@RestController
@AllArgsConstructor
public class WordCountRestService {
    private final StreamsBuilderFactoryBean factoryBean;

    @GetMapping("/count/{word}")
    public Long getWordCount(@PathVariable String word) {
        KafkaStreams kafkaStreams =  factoryBean.getKafkaStreams();
        ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams
            .store(StoreQueryParameters.fromNameAndType("counts", QueryableStoreTypes.keyValueStore()));
        return counts.get(word);
    }

    @GetMapping("/counts")
    public String getWordCounts() {
        KafkaStreams kafkaStreams =  factoryBean.getKafkaStreams();
        StringBuilder result = new StringBuilder();
        ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams
            .store(StoreQueryParameters.fromNameAndType("counts", QueryableStoreTypes.keyValueStore()));
        
        KeyValueIterator<String, Long> range = counts.all();
        while (range.hasNext()) {
            KeyValue<String, Long> next = range.next();
            System.out.println("count for " + next.key + ": " + next.value);
            result.append("'"+next.key+"' : "+next.value+"\n");
        }
        return result.toString();
    }
}

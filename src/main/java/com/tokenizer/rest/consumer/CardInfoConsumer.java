package com.tokenizer.rest.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tokenizer.rest.model.AuthModel;
import com.tokenizer.rest.producer.EncryptedCardInfoProducer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

import static com.tokenizer.rest.App.consumerProperties;
import static com.tokenizer.rest.util.Constants.TOPIC;

public class CardInfoConsumer {

    private static Logger logger = LoggerFactory.getLogger(CardInfoConsumer.class);

    private CardInfoConsumer() {
    }

    public static void consume() {
        KafkaConsumer kafkaConsumer = new KafkaConsumer(consumerProperties);
        kafkaConsumer.subscribe(Arrays.asList(consumerProperties.get(TOPIC)));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            records.forEach(r -> {
                try {
                    AuthModel authModel = new ObjectMapper().readValue(r.value(), AuthModel.class);
                    EncryptedCardInfoProducer.createProducerAndSendMessage(authModel);
                } catch (Exception e) {
                    logger.error("Failed to process request: {}", r);
                }
            });
        }
    }

}

package com.tokenizer.rest.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tokenizer.rest.crypto.Encryptor;
import com.tokenizer.rest.model.AuthModel;
import com.tokenizer.rest.model.EncryptedCardInfoModel;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.tokenizer.rest.App.producerProperties;
import static com.tokenizer.rest.util.Builders.build;
import static com.tokenizer.rest.util.Constants.ENCRYPTION_KEY;
import static com.tokenizer.rest.util.Constants.TOPIC;

public class EncryptedCardInfoProducer {

    private static final Logger logger = LoggerFactory.getLogger(EncryptedCardInfoProducer.class);

    private EncryptedCardInfoProducer() {

    }

    public static void createProducerAndSendMessage(AuthModel authModel) {
        try (KafkaProducer kpr = new KafkaProducer(producerProperties)) {
            encryptAndSendMessage(authModel, kpr);
        } catch (Exception e) {
            logger.error("Kafka producer connection error: {}, errorMessage: {}", authModel, e.getMessage());
        }
    }

    private static void encryptAndSendMessage(AuthModel authModel, KafkaProducer kpr) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String token = Encryptor.encrypt(objectMapper.writeValueAsString(build(authModel)),
                (String) producerProperties.get(ENCRYPTION_KEY));
            EncryptedCardInfoModel encryptedCardInfoModel = new EncryptedCardInfoModel(
                authModel.getTransactionId(), token);
            kpr.send(new ProducerRecord(producerProperties.getProperty(TOPIC),
                objectMapper.writeValueAsString(encryptedCardInfoModel)));
            logger.info("Encrypted and message sent successfully for txId: {}", authModel.getTransactionId());
        } catch (Exception e) {
            logger.error("Failed to process request: {}, errorMessage: {}", authModel, e.getMessage());
        }
    }

}

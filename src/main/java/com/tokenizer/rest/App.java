package com.tokenizer.rest;

import com.tokenizer.rest.config.AppConfiguration;
import com.tokenizer.rest.consumer.CardInfoConsumer;
import io.dropwizard.Application;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Properties;

import static com.tokenizer.rest.util.Constants.*;

public class App extends Application<AppConfiguration> {

	private static final Logger logger = LoggerFactory.getLogger(App.class);

	public static final Properties consumerProperties = new Properties();
	public static final Properties producerProperties = new Properties();

	@Override
	public void run(AppConfiguration c, Environment e) throws Exception
	{
		configureConsumerProperties(c);
		configureProducerProperties(c);
		configureRedis(c);
		CardInfoConsumer.consume();
	}

	private void configureConsumerProperties(AppConfiguration c) {
		consumerProperties.put(BOOTSTRAP_SERVER, c.getBootstrapServer());
        consumerProperties.put(KEY_DESERIALIZER, c.getKeyDeserializer());
        consumerProperties.put(VALUE_DESERIALIZER, c.getValueDeserializer());
        consumerProperties.put(TOPIC, c.getTopicCardInfo());
        consumerProperties.put(GROUP_ID, c.getGroupId());
	}

	private void configureProducerProperties(AppConfiguration c) {
		producerProperties.put(BOOTSTRAP_SERVER, c.getBootstrapServer());
		producerProperties.put(KEY_SERIALIZER, c.getKeySerializer());
		producerProperties.put(VALUE_SERIALIZER, c.getValueSerializer());
		producerProperties.put(TOPIC, c.getTopicEncryptedCardInfo());
		producerProperties.put(ENCRYPTION_KEY, c.getEncryptionKey());
	}

	private void configureRedis(AppConfiguration c) {
		try (Jedis jedis  = new Jedis(c.getRedisHost(), Integer.parseInt(c.getRedisPort()))) {
			jedis.set(ENCRYPTION_KEY, c.getEncryptionKey());
		}catch (Exception e) {
			logger.error("Failed to connect redis");
		}
	}

	public static void main(String[] args) throws Exception {
		new App().run(args);
	}
}

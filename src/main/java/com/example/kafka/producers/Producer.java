package com.example.kafka.producers;


import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Profile("Producer")
@Component
public class Producer {
	private KafkaProducer<String, String> producer;
	private static final int MAX_RETRIES = 10;

	@PostConstruct
	public void initProducer() {
		Properties properties = new Properties();

		String bootstrapServers = "localhost:19092,localhost:19093,localhost:19094";

		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// подтверждение от всех реплик
		properties.put(ProducerConfig.ACKS_CONFIG, "all");
		//указал что 10 повторных попыток
		properties.put(ProducerConfig.RETRIES_CONFIG, MAX_RETRIES);
		//задержка между повторными попытками 1 сек
		properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
		this.producer = new KafkaProducer<>(properties);

		String exampleTopic = "some-topic";
		String exampleKey = "key-2";
		String exampleMessage = "message-1";

		sendMessage(exampleTopic, exampleKey, exampleMessage);
		System.out.printf("Отправка сообщения [topic=%s, key=%s, message=%s]%n",
				exampleTopic, exampleKey, exampleMessage
		);
		producer.close();
	}

	private void sendMessage(String topic, String key, String message) {
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
		try {
			producer.send(record).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

}
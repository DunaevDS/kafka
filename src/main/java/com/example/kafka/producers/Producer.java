package com.example.kafka.producers;

import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Profile("producer1") // Добавляем профиль
@Component
public class Producer {
	private KafkaProducer<String, String> producer;

	public void KafkaProducer() {
		Properties properties = new Properties();

		String bootstrapServers = "localhost:9092,localhost:9093,localhost:9094";

		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		this.producer = new KafkaProducer<>(properties);

		String exampleTopic = "some-topic";
		String exampleKey = "key-1";
		String exampleMessage = "message-1";

		sendMessage(exampleTopic, exampleKey, exampleMessage);
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
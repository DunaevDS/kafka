package com.example.kafka.consumers;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


@Profile("SingleMessageConsumer") // Добавляем профиль
@Slf4j
@Component
public class SingleMessageConsumer {
	private KafkaConsumer<String, String> consumer;

	public void KafkaConsumer() {
		Properties props = new Properties();
		String bootstrapServers = "localhost:9092,localhost:9093,localhost:9094";

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "single-message-group");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		// Отключаем авто-коммит
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

		this.consumer = new KafkaConsumer<>(props);
		this.consumer.subscribe(Collections.singletonList("some-topic"));

		startConsuming();
	}

	@PreDestroy
	public void close() {
		consumer.wakeup();
	}

	private void startConsuming() {
		while (true) {
			ConsumerRecords<String, String> records =
					consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> record : records) {
				processMessage(record);
			}
		}

	}

	private void processMessage(ConsumerRecord<String, String> record) {
		try {
			// 1. Обработка сообщения
			log.info("Обработка сообщения [key={}, value={}, offset={}, partition={}]",
					record.key(), record.value(), record.offset(), record.partition());

			// 2. Вручную коммитим оффсет после успешной обработки
			Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
			offsets.put(
					new TopicPartition(record.topic(), record.partition()),
					new OffsetAndMetadata(record.offset() + 1) // +1 чтобы перейти к следующему сообщению
			);

			consumer.commitSync(offsets);
			log.info("Коммит оффсетов для: {}-{}", record.topic(), record.partition());

		} catch (Exception e) {
			log.error("Обработка сообщения зафейлилась по причине: {}", e.getMessage());
		}
	}
}

package com.example.kafka.consumers;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


@Profile("SingleMessageConsumer")
@Slf4j
@Component
public class SingleMessageConsumer {
	private KafkaConsumer<String, String> consumer;

	@PostConstruct
	public void initConsumer() {
		Properties props = new Properties();
		String bootstrapServers = "localhost:19092,localhost:19093,localhost:19094";

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "single-message-group");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		// Отключаем авто-коммит
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

		this.consumer = new KafkaConsumer<>(props);
		this.consumer.subscribe(Collections.singletonList("topic1"));

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
			System.out.printf("Обработка сообщения [key=%s, value=%s, offset=%s, partition=%s]%n",
					record.key(), record.value(), record.offset(), record.partition()
			);


			// 2. Вручную коммитим оффсет после успешной обработки
			Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
			offsets.put(
					new TopicPartition(record.topic(), record.partition()),
					new OffsetAndMetadata(record.offset() + 1) // +1 чтобы перейти к следующему сообщению
			);

			consumer.commitSync(offsets);
			System.out.printf("Коммит оффсетов для: %s-%s%n", record.topic(), record.partition());

		} catch (SerializationException e) {
			System.out.printf("Ошибка обработки сообщения [key=%s, value=%s, offset=%s, partition=%s%n] по причине: %s; %s",
					record.key(), record.value(), record.offset(), record.partition(), e.getMessage(), e);
		} catch (Exception e) {
			System.out.printf("Обработка сообщения зафейлилась по причине: %s%n", e.getMessage());
		}
	}
}

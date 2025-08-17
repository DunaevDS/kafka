package com.example.kafka.consumers;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Profile("BatchMessageConsumer") // Добавляем профиль
@Component
public class BatchMessageConsumer {
	private KafkaConsumer<String, String> consumer;
	//раз по задаче стоит минимум 10 сообщений, то пусть так
	private static final int MIN_BATCH_SIZE = 10;

	public void KafkaConsumer2() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "batch-processing-group");
		// Отключаем авто-коммит
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		// Увеличиваем максимальное количество сообщений за один poll (думаю пока что хватит и 100)
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Math.max(MIN_BATCH_SIZE, 100));
		// до 1 сек ждать батч
		props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "1000");
		// брокер вернёт не меньше 1 KB данных
		props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1024");

		this.consumer = new KafkaConsumer<>(props);
		this.consumer.subscribe(Collections.singletonList("some-topic"));

		startConsuming();
	}

	@PreDestroy
	public void close() {
		consumer.close();
	}

	private void startConsuming() {
		List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

		while (true) {
			ConsumerRecords<String, String> records =
					consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> record : records) {
				buffer.add(record);
			}

			if (buffer.size() >= MIN_BATCH_SIZE) {
				// обработка пачки
				for (ConsumerRecord<String, String> record : buffer) {
					System.out.printf("Обработка сообщения [key=%s, value=%s, offset=%d, partition=%d]%n",
							record.key(), record.value(), record.offset(), record.partition()
					);
				}

				// коммитим после обработки
				consumer.commitSync();
				buffer.clear();
			}
		}
	}
}

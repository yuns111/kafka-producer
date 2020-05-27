package com.yuns.kafka_test;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Main {
	public static void main(String[] args) {
		Properties kafkaProperties = createKafkaProperties();
		sendAsync(kafkaProperties);
	}

	private static void sendAsync(Properties kafkaProperties) {
		Producer<String, String> producer = new KafkaProducer<String, String>(kafkaProperties);

		for (int i = 0; i < 100; i++) {
			producer.send(new ProducerRecord<>("study-sample-topic", "2222", "test : " + i), ((recordMetadata, e) -> {
				if (Objects.nonNull(e)) {
					e.printStackTrace();
					return;
				}

				System.out.println(String.format("%s - [%d/%d]%s", Thread.currentThread().getName(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.topic()));
			}));
		}
		producer.close();
	}

	private static void sendSync(Properties kafkaProperties) {
		Producer<String, String> producer = new KafkaProducer<String, String>(kafkaProperties);

		for (int i = 0; i < 100; i++) {
			Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>("study-sample-topic", "test : " + i));

			try {
				RecordMetadata recordMetadata = future.get();
				System.out.println(String.format("[%d/%d]%s", recordMetadata.partition(), recordMetadata.offset(), recordMetadata.topic()));

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		producer.close();
	}

	private static Properties createKafkaProperties() {
		Properties kafkaProperties = new Properties();

		String server = "localhost:9092";

		kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
		kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "all");

		return kafkaProperties;
	}
}

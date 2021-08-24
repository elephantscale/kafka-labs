package x.lab3_api_intro;

import java.util.Arrays;
import java.util.Properties;
import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer {
	private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "group1");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("test")); // subscribe to topics

		boolean keepRunning = true;
		Duration MillisDuration = Duration.ofMillis(1000);
		logger.info("listening on test topic");
		while (keepRunning) {
			ConsumerRecords<String, String> records = consumer.poll(MillisDuration);
			if (records.count() == 0)
				continue;
			logger.debug("Got " + records.count() + " messages");
			for (ConsumerRecord<String, String> record : records) {
				logger.debug("Received message : " + record);
			}
		}
		consumer.close();
	}
}

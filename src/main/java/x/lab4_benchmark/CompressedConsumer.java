package x.lab4_benchmark;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.time.Duration;

public class CompressedConsumer implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(CompressedConsumer.class);

	private final String topic;
	private final KafkaConsumer<String, String> consumer;
	private boolean keepRunning = true;
	NumberFormat formatter = NumberFormat.getInstance();

	public CompressedConsumer(String topic) {
		this.topic = topic;
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "group1");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		this.consumer = new KafkaConsumer<>(props);
		this.consumer.subscribe(Arrays.asList(this.topic));
	}

	@Override
	public void run() {
		int numMessages = 0;
		while (keepRunning) {
			// pass the time with java.Time.Duration object as parameter
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

			// TODO-2 : calculate how many records we have got
			// replace this with records.??? (hint : count)
			int count = 0;
			if (count == 0)
				continue;
			logger.debug("Got " + count + " messages");

			for (ConsumerRecord<String, String> record : records) {
				numMessages++;
				logger.debug("Received message [" + numMessages + "] : " + record);

				// System.out.println("Received message: (" + record.key() + ", " +
				// record.value() + ") at offset " + record.offset());
			}
		}

		// logger.info(this + " received " + numMessages);
		logger.info("Received " + numMessages);

		// TODO-3 : close consumer
		// consumer.???
	}

	public void stop() {
		this.keepRunning = false;
		consumer.wakeup();
	}

	@Override
	public String toString() {
		return "ClickstreamConsumer (topic=" + this.topic + ")";
	}

	public static void main(String[] args) throws Exception {
		/*
		 * TODO-4 : create a consumer ClickstreamConsumer takes only one parameter name
		 * of topic to listen to. Set it to "clickstream"
		 */
		CompressedConsumer consumer = new CompressedConsumer("???");

		Thread t1 = new Thread(consumer);
		logger.info("starting consumer... : " + consumer);
		t1.start();
		t1.join();
		logger.info("consumer shutdown.");

	}

}

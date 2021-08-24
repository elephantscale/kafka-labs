package x.lab3_api_intro;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickstreamConsumer implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(ClickstreamConsumer.class);

	private final String topic;
	private final String goupId = "group1";
	private final KafkaConsumer<String, String> consumer;
	private boolean keepRunning = true;
	NumberFormat formatter = NumberFormat.getInstance();

	public ClickstreamConsumer(String topic) {
		this.topic = topic;
		Properties props = new Properties();
		// TODO-1 : set servers to "localhost:9092"
		props.put("bootstrap.servers", "???");
		props.put("group.id", this.goupId);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		this.consumer = new KafkaConsumer<>(props);
		this.consumer.subscribe(Arrays.asList(this.topic));
	}

	@Override
	public void run() {
		int numMessages = 0;
		while (keepRunning) {
			// TODO increase time milis time from 0 to desirable number
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));

			int count = records.count();
			if (count == 0)
				continue;
			logger.debug("Got " + count + " messages");

			for (ConsumerRecord<String, String> record : records) {
				numMessages++;
				logger.debug("Received message [" + numMessages + "] : " + record);

			}
		}

		// logger.info(this + " received " + numMessages);
		logger.info("Received " + formatter.format(numMessages));

		// TODO-3 : close consumer
		// consumer.???
		consumer.close();
	}

	public void stop() {
		this.keepRunning = false;
		consumer.wakeup();
	}

	@Override
	public String toString() {
		return "ClickstreamConsumer (topic=" + this.topic + ", group=" + this.goupId +  ")";
	}

	public static void main(String[] args) throws Exception {
		/*
		 * TODO-4 : create a consumer ClickstreamConsumer takes only one parameter name
		 * of topic to listen to. Set it to "clickstream"
		 */
		ClickstreamConsumer consumer = new ClickstreamConsumer("???");

		Thread t1 = new Thread(consumer);
		logger.info("starting consumer... : " + consumer);
		t1.start();
		t1.join();
		logger.info("consumer shutdown.");

	}

}

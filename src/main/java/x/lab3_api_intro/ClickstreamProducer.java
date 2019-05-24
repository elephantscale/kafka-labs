package x.lab3_api_intro;

import java.text.NumberFormat;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import x.utils.ClickStreamGenerator;

public class ClickstreamProducer implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(ClickstreamProducer.class);

	private final String topic;
	private final int maxMessages;
	private final int frequency;
	private final Properties props;
	private boolean keepRunning;

	NumberFormat formatter = NumberFormat.getInstance();

	private final KafkaProducer<String, String> producer;

	// topic, how many messages to send, and how often (in milliseconds)
	public ClickstreamProducer(String topic, int maxMessages, int frequency) {
		this.topic = topic;
		this.maxMessages = maxMessages;
		this.frequency = frequency;
		this.keepRunning = true;

		this.props = new Properties();
		// TODO-1 : set the broker to 'localhost:9092'
		this.props.put("bootstrap.servers", "???");

		this.props.put("client.id", "ClickstreamProducer");
		this.props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		this.props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		this.producer = new KafkaProducer<>(props);
	}

	@Override
	public void run() {

		int numMessages = 0;
		long t1, t2;
		long start = System.nanoTime();
		while (this.keepRunning && (numMessages < this.maxMessages)) {
			numMessages++;
			// String clickstream = ClickStreamGenerator.getClickstreamAsCsv();
			String clickstreamJSON = ClickStreamGenerator.getClickstreamAsJSON();

			String key = "" + numMessages;
			String value = clickstreamJSON;

			/*
			 * // TODO-2 : un-comment this block
			 * 
			 * // TODO-2 : let's construct a record
			 * 
			 * // ProducerRecord takes three parameters // - first param : topic =
			 * this.topic // - second param : key = the key just contructed // - third param
			 * : value = the value just constructored
			 * 
			 * ProducerRecord<String, String> record = new ProducerRecord<>( ???, ???, ???);
			 * 
			 * t1 = System.nanoTime(); producer.send(record); t2 = System.nanoTime();
			 * 
			 * logger.debug("sent : [" + record + "]  in " + formatter.format(t2 - t1) +
			 * " nano secs\n"); // TimeUnit.NANOSECONDS.toMillis(t2 - t1) + " ms");
			 */

			try {
				if (this.frequency > 0)
					Thread.sleep(this.frequency);
			} catch (InterruptedException e) {
			}
		}
		long end = System.nanoTime();

		// TODO-3 : end the Kafka producer. Call method 'close'
		// producer.???();

		// print summary
		logger.info("\n== " + toString() + " done.  " + formatter.format(numMessages) + " messages sent in "
				+ formatter.format((end - start) / 10e6) + " milli secs.  Throughput : "
				+ formatter.format(numMessages * 10e9 / (end - start)) + " msgs / sec");

	}

	public void stop() {
		this.keepRunning = false;
	}

	@Override
	public String toString() {
		return "ClickstreamProducer (topic=" + this.topic + ", maxMessages=" + formatter.format(this.maxMessages)
				+ ", freq=" + formatter.format(this.frequency) + " ms)";
	}

	// test driver
	public static void main(String[] args) throws Exception {

		/*
		 * TODO : uncomment this block
		 * 
		 * // TODO-4 : let's kick off the producer // ClickstreamProducer() takes three
		 * parameters // - first param : name of topic = "clickstream" // - second param
		 * : how many messages to send = 10 (start with 10 and increase later) // -
		 * third param : frequency, how often to send = 1000 (in milliseconds, 0 for no
		 * wait between sends)
		 * 
		 * // ClickstreamProducer producer = new ClickstreamProducer(???, ??? , ???);
		 * 
		 * logger.info("Producer starting.... : " + producer); Thread t1 = new
		 * Thread(producer); t1.start(); t1.join(); // wait for thread to complete
		 * logger.info("Producer done.");
		 */
	}

}

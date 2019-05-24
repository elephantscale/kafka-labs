package x.lab4_benchmark;

import java.text.NumberFormat;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import x.utils.ClickStreamGenerator;

public class CompressedProducer implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(CompressedProducer.class);

	private final String topic;
	private final int maxMessages;
	private final Properties props;
	private boolean keepRunning;
	NumberFormat formatter = NumberFormat.getInstance();

	private final KafkaProducer<String, String> producer;

	// topic, how many messages to send
	public CompressedProducer(String topic, int maxMessages) {
		this.topic = topic;
		this.maxMessages = maxMessages;
		this.keepRunning = true;

		this.props = new Properties();
		// TODO : start with your own kafka "localhost:9092"
		// once the program is working, pair up with another student
		// change the host to point to their kafka node! :-)
		// recommend using 'internal IP' address of their machine if you know it
		this.props.put("bootstrap.servers", "???");
		this.props.put("client.id", "CompressedProducer");

		this.props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		this.props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// TODO-2: set compression type to gzip
		// TODO-2a: Compare the performance by trying it with multiple codecs such as
		// gzip,snappy,lz4
		// See documentation for values to give to this property
		// https://kafka.apache.org/documentation/#configuration

//		this.props.put("compression.type", "???");

		this.producer = new KafkaProducer<>(props);
	}

	@Override
	public void run() {

		int numMessages = 0;
		long t1, t2;
		long start = System.nanoTime();
		while (this.keepRunning && (numMessages < this.maxMessages)) {
			numMessages++;
			String clickstream = ClickStreamGenerator.getClickstreamAsJSON();

			ProducerRecord<String, String> record = null;

			record = new ProducerRecord<>(this.topic, "" + numMessages, clickstream);
			t1 = System.nanoTime();
			producer.send(record);
			t2 = System.nanoTime();

			// logger.debug("sent : [" + record + "] in " + (t2 - t1) + " nano secs");

		}
		long end = System.nanoTime();

		producer.close();

		// print summary
		logger.info("\n== " + toString() + " done.  " + formatter.format(numMessages) + " messages sent in "
				+ formatter.format((end - start) / 10e6) + " milli secs.  Throughput : "
				+ formatter.format((long) (numMessages * 10e9 / (end - start))) + " msgs / sec\n");

	}

	public void stop() {
		this.keepRunning = false;
	}

	@Override
	public String toString() {
		return "Compression Benchmark  Producer (topic=" + this.topic + ", maxMessages="
				+ formatter.format(this.maxMessages) + ")";
	}

	// test driver
	public static void main(String[] args) throws Exception {

		CompressedProducer producer = null;
		// TODO : start with 1000 messages, and then increase it to a million (1000000)
		producer = new CompressedProducer("benchmark", 1000);

		logger.info("Producer starting.... : " + producer);
		Thread t1 = new Thread(producer);
		t1.start();
		t1.join(); // wait for thread to complete
		logger.info("Producer done.");

	}
	
	// BONUS Lab : 
	//  compression can help to reduce the size of data we are sending out
	//  can you figure out a way to find the amount of data sent by the producer?
	//  you may want to use OS level tools to measure this?
	//  share what you find with the class?

}

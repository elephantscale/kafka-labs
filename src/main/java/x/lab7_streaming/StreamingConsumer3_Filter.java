/// Filter out the streams
package x.lab7_streaming;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import x.utils.MyConfig;

public class StreamingConsumer3_Filter {
	private static final Logger logger = LoggerFactory.getLogger(StreamingConsumer3_Filter.class);

	public static void main(String[] args) {

		Properties config = new Properties();
		// "bootstrap.servers" = "localhost:9092"
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MyConfig.DEFAULT_BOOTSTRAP_SERVERS);
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streaming-consumer2");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		// Records should be flushed every 10 seconds. This is less than the
		// default
		// in order to keep this example interactive.
		config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		// For illustrative purposes we disable record caches
		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

		final StreamsBuilder builder = new StreamsBuilder();

		 //# TODO-1 : construct KStream
	    //#     param 1 : topic name  : "clickstream"
	    final KStream<String, String> clickstream = builder.stream("???");

	    // print to console
		clickstream.print(Printed.toSysOut());

		//# TODO-2 : filter clicks only
		// Hint the pattern you are looking for is : "clicked"
		final KStream<String, String> actionClickedStream = clickstream
				.filter((k, v) -> v.contains("???"));
		
		// # TODO-3 : print clickstream to console
		// actionClickedStream.print(???);

		// start the stream
		final KafkaStreams streams = new KafkaStreams(builder.build(), config);
		streams.cleanUp();
		streams.start();

		logger.info("kstreams starting on " + MyConfig.TOPIC_CLICKSTREAM);

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

}

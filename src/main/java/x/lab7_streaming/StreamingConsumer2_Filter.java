/// Filter out the streams
package x.lab7_streaming;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import x.utils.MyConfig;

public class StreamingConsumer2_Filter {
	private static final Logger logger = LogManager.getLogger();

	public static void main(String[] args) {

		Properties config = new Properties();
		// "bootstrap.servers" = "localhost:9092"
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MyConfig.DEFAULT_BOOTSTRAP_SERVERS);
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streaming-consumer2");
		config.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		config.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		// Records should be flushed every 10 seconds. This is less than the
		// default
		// in order to keep this example interactive.
		config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		// For illustrative purposes we disable record caches
		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

		final KStreamBuilder builder = new KStreamBuilder();

		final KStream<String, String> clickstream = builder.stream(Serdes.String(), Serdes.String(),
				MyConfig.TOPIC_CLICKSTREAM);
		clickstream.print("KStream-clickstream");

		// TODO-1 : filter clicks only
		// Hint the pattern you are looking for is : "\"action\":\"clicked\""
		final KStream<String, String> actionClickedStream = clickstream
				.filter((k, v) -> v.contains("???"));
		actionClickedStream.print("KStream-filtered-CLICKED");

		
		// start the stream
		final KafkaStreams streams = new KafkaStreams(builder, config);
		streams.cleanUp();
		streams.start();

		logger.info("kstreams starting on " + MyConfig.TOPIC_CLICKSTREAM);

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

}

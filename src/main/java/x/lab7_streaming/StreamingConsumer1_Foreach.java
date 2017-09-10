/// Simple Streaming Consumer
package x.lab7_streaming;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;

import x.utils.MyConfig;
import x.utils.MyMetricsRegistry;

public class StreamingConsumer1_Foreach {
	private static final Logger logger = LoggerFactory.getLogger(StreamingConsumer1_Foreach.class);

	public static void main(String[] args) {

		Properties config = new Properties();
		// "bootstrap.servers" = "localhost:9092"
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MyConfig.DEFAULT_BOOTSTRAP_SERVERS);
//		config.put("group.id", "group1");
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streaming-consumer1");
		config.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		config.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		// Records should be flushed every 10 seconds. This is less than the
		// default
		// in order to keep this example interactive.
		config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		// For illustrative purposes we disable record caches
		config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

		final KStreamBuilder builder = new KStreamBuilder();

		KStream<String, String> clickstream = null;
		//# TODO-1 : construct KStream
		//# Param 1 : key serializer : Serdes.String()
		//# Param 2 : value serializer : Serdes.String()
		//# Param 3 : topic  : MyConfig.TOPIC_CLICKSTREAM

		// clickstream = builder.stream(??? , ??? , ???) ;

		//# TODO-2 : print out clickstreasm, give it a name
		clickstream.print("???");

		// process events one by one
		clickstream.foreach(new ForeachAction<String, String>() {
			public void apply(String key, String value) {
				//# TODO-3 : print out the record (key and value)
				//logger.debug("key:" + ??? + ", value:" + ???);
			}
		});

		// start the stream
		final KafkaStreams streams = new KafkaStreams(builder, config);
		streams.cleanUp();
		streams.start();
		logger.info("kstreams starting on " + MyConfig.TOPIC_CLICKSTREAM);

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

}

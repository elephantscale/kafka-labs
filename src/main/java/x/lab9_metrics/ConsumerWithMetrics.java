package x.lab9_metrics;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Meter;

import x.utils.MyConfig;
import x.utils.MyMetricsRegistry;
import x.utils.MyUtils;

public class ConsumerWithMetrics {
	private static final Logger logger = LogManager.getLogger();
	
	// TODO-1 : get a meter with name 'consumer.events'
	private static final Meter meterConsumerEvents = MyMetricsRegistry.metrics.meter("???");
	private static long eventsReceived = 0;

	public static void main(String[] args) {

		Properties config = new Properties();
		// "bootstrap.servers" = "localhost:9092"
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MyConfig.DEFAULT_BOOTSTRAP_SERVERS);
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "clickstream-traffic-reporter1");
		config.put(ConsumerConfig.GROUP_ID_CONFIG, "traffic-reporter1");
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
		// clickstream.print();

		// process each record and report traffic
		clickstream.foreach(new ForeachAction<String, String>() {
			public void apply(String key, String value) {
				eventsReceived ++;
				logger.debug("received # " + eventsReceived + ",  key:" + key + ", value:" + value);

				// TODO-2 : mark the meter for received events
				// meterConsumerEvents.???
				
				MyUtils.randomDelay(100, 500);
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

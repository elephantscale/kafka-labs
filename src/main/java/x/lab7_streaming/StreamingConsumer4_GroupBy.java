/// Filter out the streams
package x.lab7_streaming;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import x.utils.ClickstreamData;
import x.utils.MyConfig;

public class StreamingConsumer4_GroupBy {
	private static final Logger logger = LogManager.getLogger();

	public static void main(String[] args) {

		Properties config = new Properties();
		// "bootstrap.servers" = "localhost:9092"
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MyConfig.DEFAULT_BOOTSTRAP_SERVERS);
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streaming-consumer4");
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

		// input::
		// key=domain,
		// value =
		// {"timestamp":1451635200005,"session":"session_251","domain":"facebook.com","cost":91,"user":"user_16","campaign":"campaign_5","ip":"ip_67","action":"clicked"}
		//
		// mapped output
		// key = action
		// value = 1
		final Gson gson = new Gson();
		final KStream<String, Integer> actionStream = clickstream
				.map(new KeyValueMapper<String, String, KeyValue<String, Integer>>() {
					public KeyValue<String, Integer> apply(String key, String value) {
						ClickstreamData clickstream = gson.fromJson(value, ClickstreamData.class);
//						logger.debug("map() : got : " + value);
						String action = (clickstream.action != null) && (!clickstream.action.isEmpty())
								? clickstream.action : "unknown";
						KeyValue<String, Integer> actionKV = new KeyValue<>(action, 1);
//						logger.debug("map() : returning : " + actionKV);
						return actionKV;
					}
				});
		actionStream.print("KStream-Action");

		//// TODO-1 :  aggregate and count actions
		//// we have to explicity state the K,V serdes in groupby, as the types are changing
		////   -    groupByKey (Serdes.String(),   Serdes.Integer())
		////   -    count("actionCount") : this is the storage to keep counts
		final KTable<String, Long> actionCount = actionStream.
									groupByKey(Serdes.String(), Serdes.Integer())
									.count("actionCount");
		
		//// TODO-2 : print out action counts with name/prefix : KTable-ACtionCount
		// actionCount.???
		
		//# TODO-3 : lets write the data into another topic
		//# param 1 : key serializer : Serdes.String()
		//# param 2 : value serializer:  Serdes.Long
		//# param 3 : topic : MyConfig.TOPIC_ACTION_COUNT

		// actionCount.to(???, ???, ???)

		// start the stream
		final KafkaStreams streams = new KafkaStreams(builder, config);
		streams.cleanUp();
		streams.start();

		logger.info("kstreams starting on " + MyConfig.TOPIC_CLICKSTREAM);

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

}

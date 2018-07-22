/// Filter out the streams
package x.lab7_streaming;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Printed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import x.utils.ClickstreamData;
import x.utils.MyConfig;

public class StreamingConsumer5_GroupBy {
	private static final Logger logger = LoggerFactory.getLogger(StreamingConsumer5_GroupBy.class);

	public static void main(String[] args) {

		Properties config = new Properties();
		// "bootstrap.servers" = "localhost:9092"
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MyConfig.DEFAULT_BOOTSTRAP_SERVERS);
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streaming-consumer4");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		// config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
		// Serdes.Integer().getClass().getName());
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
		
		clickstream.print(Printed.toSysOut());

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
						try {
							ClickstreamData clickstream = gson.fromJson(value, ClickstreamData.class);
							logger.debug("map() : got : " + value);
							String action = (clickstream.action != null) && (!clickstream.action.isEmpty())
									? clickstream.action
									: "unknown";
							KeyValue<String, Integer> actionKV = new KeyValue<>(action, 1);
							logger.debug("map() : returning : " + actionKV);
							return actionKV;
						} catch (Exception ex) {
							// logger.error("",ex);
							logger.error("Invalid JSON : \n" + value);
							return new KeyValue<String, Integer>("unknown", 1);
						}
					}
				});
		actionStream.print(Printed.toSysOut());

		// Now aggregate and count actions
		// we have to explicity state the K,V serdes in groupby, as the types are
		// changing

		//# TODO-1 : aggregate
		//#   param1 : key type :  Serdes.String()
		//#   param2 : value type :  Serdes.Integer()
		/*
		KGroupedStream<String, Integer> grouped = actionStream.groupByKey(Serialized.with(
					Serdes.String(), // key
					Serdes.Integer()) // value
					);
		*/
		
		//# TODO-2 : count grouped stream
		//# Hint : grouped.count()
		/*
		
		final KTable<String, Long> actionCount = grouped.???();
		actionCount.toStream().print(Printed.toSysOut());
		*/

		//# BONUS lab 1 : 
		// lets write the data into another topic
		/* 
		  actionCount.to(Serdes.String(), Serdes.Long(), MyConfig.TOPIC_ACTION_COUNT);
		 */

		// start the stream
		final KafkaStreams streams = new KafkaStreams(builder.build(), config);
		streams.cleanUp();
		streams.start();

		logger.info("kstreams starting on " + MyConfig.TOPIC_CLICKSTREAM);

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

}
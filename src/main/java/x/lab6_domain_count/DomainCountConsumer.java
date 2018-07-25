package x.lab6_domain_count;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import x.utils.ClickstreamData;
import x.utils.MyConfig;

public class DomainCountConsumer {
	private static final Logger logger = LoggerFactory.getLogger(DomainCountConsumer.class);

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "group1");
    props.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList(MyConfig.TOPIC_CLICKSTREAM)); // subscribe
                                                                  // to topics
    Map<String, Integer> domainCount = new HashMap<>();
    Gson gson = new Gson();
    boolean keepRunning = true;
		while (keepRunning) {
			ConsumerRecords<String, String> records = consumer.poll(1000);
      if (records.count() == 0) continue;
			logger.debug ("Got " + records.count() + " messages");
			for (ConsumerRecord<String, String> record : records) {
				try{
					logger.debug("Received message : " + record);

					//# TODO-1 : extract the JSON string from record
					//# Hint : record.value()
					String clickstreamJSON = "";

					ClickstreamData clickstream =
					gson.fromJson(clickstreamJSON, ClickstreamData.class);

					//# TODO-2 : extract the domain attribute
					//# get it from key : record.key()
					//# or get it from clickstream  object
					String domain = "???";

					//# TODO-3  figure out a way to count domains
					//# Hint : Use a HashMap
					// int currentCount = domainCount.getOrDefault(domain, 0);
					//# update the count
					// domainCount.put (domain, ??? + 1);

					//# pretty print hashmap
					logger.info("Domain Count is \n"
					+ Arrays.toString(domainCount.entrySet().toArray()));

				}
				catch (Exception ex) {
					logger.error("", ex);
				}
			} // end for
		} // end while
    consumer.close();
  }
}

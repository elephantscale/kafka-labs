package x.lab6_domain_count;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import x.utils.ClickstreamData;
import x.utils.ClickstreamProducer;
import x.utils.MyConfig;

public class DomainCountConsumer {
	private static final Logger logger = LogManager.getLogger();

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
      logger.debug ("Got " + records.count() + " messages");
      for (ConsumerRecord<String, String> record : records) {
        logger.debug("Received message : " + record);
        
        // TODO-1 : extract the JSON string from record
        // Hint : record.value()
        String clickstreamJSON = "";
        
        // TODO-2 : parse JSON string using GSON library
        // Hint : take a look at ClickStreamProducer around line 30
        ClickstreamData clickstream = null;

        // TODO-3 : extract the domain attribute
        // get it from key : record.key()
        // or get it from clickstream JSON object
        String domain = "???";
        
        // TODO-1  figure out a way to count domains
        // Hint : Use a HashMap
        // int currentCount = domainCount.getOrDefault(domain, 0);
        // update the count
        // domainCount.put (domain, ??? + 1);
        
        // pretty print hashmap
           logger.info("Domain Count is \n"
            + Arrays.toString(domainCount.entrySet().toArray()));
         
      }
    }
    consumer.close();
  }
}
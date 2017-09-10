package x.lab3_api_intro;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer {
	private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "group1");
    props.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList("test")); // subscribe to topics

    boolean keepRunning = true;
    logger.info("listening on test topic");
    while (keepRunning) {
      ConsumerRecords<String, String> records = consumer.poll(1000);
      if (records.count() == 0) continue;
      logger.debug("Got " + records.count() + " messages");
      for (ConsumerRecord<String, String> record : records) {
        logger.debug("Received message : " + record);
      }
    }
    consumer.close();
  }
}

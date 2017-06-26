package x.lab_3;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SimpleConsumer {
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
    consumer.subscribe(Arrays.asList("test")); // subscribe to topics

    boolean keepRunning = true;
    logger.info("listening on test topic");
    while (keepRunning) {
      ConsumerRecords<String, String> records = consumer.poll(1000);
      logger.debug("Got " + records.count() + " messages");
      for (ConsumerRecord<String, String> record : records) {
        logger.debug("Received message : " + record);
      }
    }
    consumer.close();
  }
}

package x.lab05_offsets;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;

import x.utils.MyConfig;

public class SeekingConsumer {
	private static final Logger logger = LoggerFactory.getLogger(SeekingConsumer.class);
	private static final String TOPIC = "clickstream";

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "seeking1");
    props.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(TOPIC)); // subscribe

    logger.info("listening on  topic : " + TOPIC);

    TopicPartition partition = new TopicPartition(TOPIC, 0);

    int read = 0;
    while (read < 5) {
      ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100));
      long position = consumer.position(partition);
      logger.debug ("position " + position);
      for (ConsumerRecord<Integer, String> record : records) {
        read++;
        logger.debug("Received message : " + record);
        break; // only process first message
      }

      /* TODO- go to specific offsets
       *    - read the first message
       *    - read the last message
       *    - read message at offset 5
       *
       *  Reference : look at various seek options available here
       *  https://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html
       */
      logger.debug("seeking to beginning of partition " + partition);
      consumer.seekToBeginning(Collections.singletonList(partition));

      // logger.debug ("seeking to end of partition " + partition);
      // ???


      // logger.debug ("seeking to position #5 of " + partition);
      // ???

    }
    consumer.close();
  }
}

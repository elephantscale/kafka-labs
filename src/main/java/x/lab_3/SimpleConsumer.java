package x.lab_3;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SimpleConsumer {

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "group1");
    props.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList("test")); // subscribe to topics

    boolean keepRunning = true;
    System.out.println("listening on test topic");
    while (keepRunning) {
      ConsumerRecords<Integer, String> records = consumer.poll(1000);
      System.out.println("Got " + records.count() + " messages");
      for (ConsumerRecord<Integer, String> record : records) {
        System.out.println("Received message : " + record);
      }
    }
    consumer.close();
  }
}

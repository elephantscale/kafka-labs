package x.lab_5;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class JumpingConsumer {

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "group1");
    props.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(GreetingsProducer.TOPIC)); // subscribe to
                                                                // topics

    System.out.println("listening on  topic : " + GreetingsProducer.TOPIC);
    
    TopicPartition partition = new TopicPartition(GreetingsProducer.TOPIC, 0);

    int read = 0;
    while (read < 5) {
      ConsumerRecords<Integer, String> records = consumer.poll(1000);
      long position = consumer.position(partition);
      System.out.println ("position " + position);
      for (ConsumerRecord<Integer, String> record : records) {
        read++;
        System.out.println("Received message : " + record);
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
      System.out.println("seeking to beginning of partition " + partition);
      consumer.seekToBeginning(Collections.singletonList(partition));

      // System.out.println("seeking to end of partition " + partition);
      // ???
      
      
      // System.out.println("seeking to position #5 of " + partition);
      // ???

    }
    consumer.close();
  }
}

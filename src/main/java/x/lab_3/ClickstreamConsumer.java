package x.lab_3;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ClickstreamConsumer implements Runnable {

  private final String topic;
  private final KafkaConsumer<Integer, String> consumer;
  private boolean keepRunning = true;

  public ClickstreamConsumer(String topic) {
    this.topic = topic;
    Properties props = new Properties();
    // TODO-1 : set servers to  "localhost:9092"
    props.put("bootstrap.servers", "???");
    props.put("group.id", "group1");
    props.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    this.consumer = new KafkaConsumer<>(props);
    this.consumer.subscribe(Arrays.asList(this.topic));
  }

  @Override
  public void run() {
    int numMessages = 0;
    while (keepRunning) {
      ConsumerRecords<Integer, String> records = consumer.poll(1000);
      // TODO-2 : iterate over 'records'  (substitute 'records' for ???)
      for (ConsumerRecord<Integer, String> record : ???) {
        numMessages++;
        System.out
            .println("Received message [" + numMessages + "] : " + record);

        // System.out.println("Received message: (" + record.key() + ", " +
        // record.value() + ") at offset " + record.offset());
      }
    }

    System.out.println(this + " received " + numMessages);

    // TODO-3 : close consumer
    // consumer.???
  }

  public void stop() {
    this.keepRunning = false;
    consumer.wakeup();
  }

  @Override
  public String toString() {
    return "ClickstreamConsumer (topic=" + this.topic + ")";
  }

  public static void main(String[] args) throws Exception {
    /* TODO-4 : create a consumer
     *    ClickstreamConsumer takes only one parameter
     *    name of topic to listen to.  Set it to "clickstream"
     */
    ClickstreamConsumer consumer = new ClickstreamConsumer("???");

    Thread t1 = new Thread(consumer);
    System.out.println("starting consumer... : " + consumer);
    t1.start();
    t1.join();
    System.out.println("consumer shutdown.");

  }

}

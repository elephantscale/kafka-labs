package x.lab3_api_intro;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AtLeastOnceConsumer implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(AtLeastOnceConsumer.class);

  private final String topic;
  private final KafkaConsumer<String, String> consumer;
  private boolean keepRunning = true;

  public AtLeastOnceConsumer(String topic) {
    this.topic = topic;
    Properties props = new Properties();
    /*
     * To implement AtLeast Once processing, we need to turn off auto-commit
     * and manually commit the offsets
     */

    // TODO-1 : set servers to  "localhost:9092"
    props.put("bootstrap.servers", "???");
    props.put("group.id", "group1");
    props.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");

    // TODO-2: Set enable.auto.commit to false
      // props.put(?);
    this.consumer = new KafkaConsumer<>(props);
    this.consumer.subscribe(Arrays.asList(this.topic));
  }

  @Override
  public void run() {
    int numMessages = 0;
    while (keepRunning) {

      // TODO-3 : set poll interval to a suitable value - NOT 0
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));

      // TODO-4 : find how many records we have got
      int count = 0 ;  // replace this with records.???
      if (count == 0) continue;
      logger.debug("Got " + count + " messages");

      for (ConsumerRecord<String, String> record : records) {
        numMessages++;
        logger.debug("Received message [" + numMessages + "] : " + record);        
      }
      // TODO-4: call consumer.commitSync() to manually commit the offset

    }

    //logger.info(this + " received " + numMessages);
    logger.info("Received " + numMessages);

    // TODO-5 : close consumer
    // consumer.???
  }

  public void stop() {
    this.keepRunning = false;
    consumer.wakeup();
  }

  @Override
  public String toString() {
    return "AtLeastOnceConsumer (topic=" + this.topic + ")";
  }

  public static void main(String[] args) throws Exception {
    /* TODO-5 : create a consumer
     *    AtleastonceConsumer takes only one parameter
     *    name of topic to listen to.  Set it to "clickstream"
     */
    AtLeastOnceConsumer consumer = new AtLeastOnceConsumer("???");

    Thread t1 = new Thread(consumer);
    logger.info("starting consumer... : " + consumer);
    t1.start();
    t1.join();
    logger.info("consumer shutdown.");

  }

}

package x.lab5_offsets;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManualOffsetConsumer implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(ManualOffsetConsumer.class);

  private final String topic;
  private final KafkaConsumer<String, String> consumer;
  private boolean keepRunning = true;

  public ManualOffsetConsumer(String topic) {
    this.topic = topic;
    Properties props = new Properties();
    /*
     * To implement AtLeast Once processing, we need to turn off auto-commit
     * and manually commit the offsets
     */

    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "group_manual_offset");
    props.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");

    // TODO-1: Set 'enable.auto.commit' to 'false'
    props.put("???", "???");
    this.consumer = new KafkaConsumer<>(props);
    this.consumer.subscribe(Arrays.asList(this.topic));
  }

  @Override
  public void run() {
    int numMessages = 0;
    while (keepRunning) {

      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      int count = records.count() ; 
      if (count == 0) continue;
      
      logger.debug("Got " + count + " messages");

      for (ConsumerRecord<String, String> record : records) {
        numMessages++;
        logger.debug("Received message [" + numMessages + "] : " + record);        
      }
      
      // print offsets
      Set<TopicPartition> partitions = consumer.assignment();
      for (TopicPartition p : partitions) {
    	  long pos = consumer.position(p);
    	  logger.debug("OFFSET : partition:" + p.partition() + ", offset:" + pos);
      }
      
      
      /*
       TODO-2: 
      		- do a run without calling 'commitSync' 
      		- run 'ClickStreamProducer' to send some events
      		- run this code again a couple of times... 
      		- how many events are you getting?
      		- can you explain the behavior ?
      		- now uncomment the following 'commitSync' code and run again
      		- do a couple of runs, what is the behavior now?
	*/
      //      consumer.commitSync();

    }

    logger.info("Received " + numMessages);

     consumer.close();
  }

  public void stop() {
    this.keepRunning = false;
    consumer.wakeup();
  }

  @Override
  public String toString() {
    return this.getClass().getName() + " (topic=" + this.topic + ")";
  }

  public static void main(String[] args) throws Exception {
    ManualOffsetConsumer consumer = new ManualOffsetConsumer("clickstream");

    Thread t1 = new Thread(consumer);
    logger.info("starting consumer... : " + consumer);
    t1.start();
    t1.join();
    logger.info("consumer shutdown.");

  }

}

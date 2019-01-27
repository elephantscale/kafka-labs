package x.lab4_benchmark;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import x.utils.ClickStreamGenerator;
/*
 * 
 * */
 import x.utils.ClickStreamGenerator;

import x.utils.MyConfig;

public class CompressedProducer implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(CompressedProducer.class);

  private final String topic;
  private final int maxMessages;
  private final int frequency;
  private final Properties props;
  private boolean keepRunning;

  private final KafkaProducer<String, String> producer;

  // topic, how many messages to send, and how often (in milliseconds)
  public CompressedProducer(String topic, int maxMessages, int frequency) {
    this.topic = topic;
    this.maxMessages = maxMessages;
    this.frequency = frequency;
    this.keepRunning = true;

    this.props = new Properties();
    // TODO-1 : set the broker to 'localhost:9092'

    this.props.put("bootstrap.servers", "localhost:9092");
    this.props.put("client.id", "ClickstreamProducer");
    
    this.props.put("key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");
    this.props.put("value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");

    // TODO-2: set compression type to gzip
    // TODO-2a: Compare the performance by trying it with multiple codecs such as gzip,snappy,lz4
    // See documentation for values to give to this property

    this.props.put("compression.type", "?");


    this.producer = new KafkaProducer<>(props);
  }

  @Override
  public void run() {

    int numMessages = 0;
    long t1, t2;
    long start = System.nanoTime();
    while (this.keepRunning && (numMessages < this.maxMessages)) {
      numMessages++;
      String clickstream = ClickStreamGenerator.getClickstreamAsJSON();
      
      ProducerRecord<String, String> record = null;

    
      record =   new ProducerRecord<>( this.topic, "" + numMessages,"clickstream");
      t1 = System.nanoTime();
      producer.send(record);
      t2 = System.nanoTime();

      logger.debug("sent : [" + record + "]  in " + (t2 - t1) + " nano secs");
    
      try {
        if (this.frequency > 0)
          Thread.sleep(this.frequency);
      } catch (InterruptedException e) {
      }
    }
    long end = System.nanoTime();
    
    
    producer.close();

    // print summary
    logger.info("\n== " + toString() + " done.  " + numMessages + " messages sent in "
        + (end - start) / 10e6 + " milli secs.  Throughput : "
        + numMessages * 10e9 / (end - start) + " msgs / sec");

  }

  public void stop() {
    this.keepRunning = false;
  }

  @Override
  public String toString() {
    return "ClickstreamProducer (topic=" + this.topic + ", maxMessages="
        + this.maxMessages + ", freq=" + this.frequency + " ms)";
  }

  // test driver
  public static void main(String[] args) throws Exception {

    CompressedProducer producer = null;
    producer = new CompressedProducer("clickstream",  50 , 1000);

    logger.info("Producer starting.... : " + producer);
    Thread t1 = new Thread(producer);
    t1.start();
    t1.join(); // wait for thread to complete
    logger.info("Producer done.");

  }

}

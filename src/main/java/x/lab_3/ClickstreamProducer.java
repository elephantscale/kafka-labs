package x.lab_3;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import x.utils.ClickStreamGenerator;

public class ClickstreamProducer implements Runnable {
	private static final Logger logger = LogManager.getLogger();

  private final String topic;
  private final int maxMessages;
  private final int frequency;
  private final Properties props;
  private boolean keepRunning;

  private final KafkaProducer<String, String> producer;

  // topic, how many messages to send, and how often (in milliseconds)
  public ClickstreamProducer(String topic, int maxMessages, int frequency) {
    this.topic = topic;
    this.maxMessages = maxMessages;
    this.frequency = frequency;
    this.keepRunning = true;

    this.props = new Properties();
    // TODO-1 : set the broker to 'localhost:9092'   
    this.props.put("bootstrap.servers", "???");
    this.props.put("client.id", "ClickstreamProducer");
    this.props.put("key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");
    this.props.put("value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");
    this.producer = new KafkaProducer<>(props);
  }

  @Override
  public void run() {

    int numMessages = 0;
    long t1, t2;
    long start = System.nanoTime();
    while (this.keepRunning && (numMessages < this.maxMessages)) {
      numMessages++;
      String clickstream = ClickStreamGenerator.getClickstreamAsCsv();
      //String clickstream = ClickStreamGenerator.getClickstreamAsJSON();
      
      ProducerRecord<String, String> record = null;

      /* TODO-2 : let's construct a record
       *   ProducerRecord takes three parameters
       *   - first param : topic = this.topic
       *   - second param : key = numMessages  (our counter)
       *   - third param : value = clickstream
       */
      //record =   new ProducerRecord<>( ???,   ???,   ???);
      t1 = System.nanoTime();
      producer.send(record);
      t2 = System.nanoTime();

      logger.debug("sent : [" + record + "]  in " + (t2 - t1) + " nano secs");
      // TimeUnit.NANOSECONDS.toMillis(t2 - t1) + " ms");

      try {
        if (this.frequency > 0)
          Thread.sleep(this.frequency);
      } catch (InterruptedException e) {
      }
    }
    long end = System.nanoTime();
    
    // TODO-3 : end the Kafka producer.  Call method 'close'
    // producer.???();

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

    ClickstreamProducer producer = null;

    /* TODO-4 :  let's kick off the producer
     * ClickstreamProducer() takes three parameters
     * - first param : name of topic = "clickstream"
     * - second param : how many messages to send = 10 (start with 10 and increase later)
     * - third param : frequency, how often to send = 1000  (in milliseconds,  0 for no wait between sends)
     */
    // producer = new ClickstreamProducer(???,  ??? , ???);

    logger.info("Producer starting.... : " + producer);
    Thread t1 = new Thread(producer);
    t1.start();
    t1.join(); // wait for thread to complete
    logger.info("Producer done.");

  }

}

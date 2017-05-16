package x.lab_4;


import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import x.utils.ClickStreamGenerator;
import x.utils.MyConfig;

enum SendMode {
  SYNC, ASYNC, FIRE_AND_FORGET
}

public class BenchmarkProducer implements Runnable, Callback {
	private static final Logger logger = LogManager.getLogger();

  private final String topic;
  private final int maxMessages;
  private final SendMode sendMode;
  private final Properties props;

  private final KafkaProducer<Integer, String> producer;

  // topic, how many messages to send, and send mode
  public BenchmarkProducer(String topic, int maxMessages, SendMode sendMode) {
    this.topic = topic;
    this.maxMessages = maxMessages;
    this.sendMode = sendMode;

    this.props = new Properties();
    this.props.put("bootstrap.servers", "localhost:9092");
    this.props.put("client.id", "BenchmarkProducer");
    this.props.put("key.serializer",
        "org.apache.kafka.common.serialization.IntegerSerializer");
    this.props.put("value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");
    this.producer = new KafkaProducer<>(props);
  }

  @Override
  public void run() {

    int numMessages = 0;
    long t1, t2;
    long start = System.nanoTime();
    while ((numMessages < this.maxMessages)) {
      numMessages++;
      String clickstream = ClickStreamGenerator.getClickstreamAsCsv();
      // String clickstream = ClickStreamGenerator.getClickstreamAsJSON();
      ProducerRecord<Integer, String> record =
          new ProducerRecord<>(this.topic, numMessages, clickstream);
      t1 = System.nanoTime();
      try {
        /* TODO - send events the correct way
         *  - fire and forget : producer.send(record)
         *  - sync : producer.send(record).get()  // wait for reply
         *  - async : producer.send (record, this)   // provide call back
         * 
         */
        switch (this.sendMode) {
        case FIRE_AND_FORGET:
          // TODO : send here
          break;
        case SYNC:
          // TODO : send here
          break;
        case ASYNC:
          // TODO : send here
          break;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      t2 = System.nanoTime();

      logger.debug(
          "sent : [" + record + "]  in " + (t2 - t1) / 10e6 + " milli secs");
      // TimeUnit.NANOSECONDS.toMillis(t2 - t1) + " ms");

    }
    long end = System.nanoTime();
    producer.close(); // close connection

    // print summary
    logger.info(
        "== " + toString() + " done.  " + numMessages + " messages sent in "
            + (end - start) / 10e6 + " milli secs.  Throughput : "
            + numMessages * 10e9 / (end - start) + " msgs / sec");

  }

  @Override
  public String toString() {
    return "ClickstreamProducer (topic=" + this.topic + ", maxMessages="
        + this.maxMessages + ", sendMode=" + this.sendMode + ")";
  }

  // Kafka callback
  @Override
  public void onCompletion(RecordMetadata meta, Exception ex) {
    if (ex != null) {
      logger.error("Callback :  Error during async send");
      ex.printStackTrace();
    }
    if (meta != null) {
      logger.debug("Callback : Success sending message " + meta);
    }

  }

  // test driver
  public static void main(String[] args) throws Exception {

    for (SendMode sendMode : SendMode.values()) {
      BenchmarkProducer producer =
          new BenchmarkProducer(MyConfig.TOPIC_BENCHMARK, 10000, sendMode);
      logger.info("== Producer starting.... : " + producer);
      Thread t1 = new Thread(producer);
      t1.start();
      t1.join(); // wait for thread to complete
      logger.info("== Producer done.");
    }

  }

}

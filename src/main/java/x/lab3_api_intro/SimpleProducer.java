package x.lab3_api_intro;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import x.utils.MyConfig;

public class SimpleProducer {
	private static final Logger logger = LogManager.getLogger();

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("client.id", "SimpleProducer");
    props.put("key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer<String, String> producer = new KafkaProducer<>(props);

    String topic = MyConfig.TOPIC_TEST; // "test"
    String key = new Integer(1).toString();
    String value = "Hello world";
    ProducerRecord<String, String> record =
        new ProducerRecord<>(topic, key, value);
    // option 1 : fire and forget
    logger.debug("sending : " + record);
    producer.send(record);

    /*
    // option 2 : sync
    Future<RecordMetadata> future = producer.send(record);
    RecordMetadata recordMetaData = future.get();
    producer.send(record).get();

    // option 3 : async
    producer.send(record, new KafkaCallback());
    */

    producer.close();

  }

}

class KafkaCallback implements Callback {
	private static final Logger logger = LogManager.getLogger();

  @Override
  public void onCompletion(RecordMetadata meta, Exception ex) {
    if (ex != null) // error
      ex.printStackTrace();

    if (meta != null) // success
      logger.debug("send success");
  }
}

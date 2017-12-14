package x.lab3_api_intro;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import x.utils.MyConfig;

public class SimpleProducer {
	private static final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);

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
    logger.debug("sending : " + record);
    producer.send(record);

    producer.close();

  }

}

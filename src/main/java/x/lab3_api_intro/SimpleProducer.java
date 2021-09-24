package x.lab3_api_intro;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SimpleProducer {
	private static final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "SimpleProducer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    
    KafkaProducer<String, String> producer = new KafkaProducer<>(props);

    String topic = "test";
    String key = "" + System.currentTimeMillis();
    String value = "Hello world";
    ProducerRecord<String, String> record =
        new ProducerRecord<>(topic, key, value);
    
    long t1 = System.nanoTime();
    RecordMetadata meta = producer.send(record).get();
    long t2 = System.nanoTime();
    
    logger.debug(String.format("Sent record (key:%s, value:%s), "
    		+ "meta (partition=%d, offset=%d, timestamp=%d), "
    		+ "time took = %.2f ms",
    		key, value, meta.partition(), meta.offset(), meta.timestamp(),
    		(t2-t1)/1e6));


    producer.close();

  }

}

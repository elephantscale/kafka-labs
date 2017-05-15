package x.utils;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

public class ClickstreamProducer {
	private static final Logger logger = LogManager.getLogger();
  public final static String TOPIC = "clickstream2";

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("client.id", "ClickstreamProducer");
    props.put("key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer<String, String> producer = new KafkaProducer<>(props);

    Gson gson = new Gson();

    for (int i = 0; i < 10; i++) {
      String clickstreamJSON = ClickStreamGenerator.getClickstreamAsJSON();
      ClickstreamData clickstream =
          gson.fromJson(clickstreamJSON, ClickstreamData.class);
      
      // TODO-1 : what is the key ?
      String key = "???";
      
      // TODO-2 : send the clickstreamJSON data as value with DOMAIN as key
      ProducerRecord<String, String> record =
          new ProducerRecord<>(TOPIC, key,  "????");
      logger.debug("sending : " + record);
      producer.send(record);

    }

    producer.close();

  }

}

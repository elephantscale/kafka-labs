package x.utils;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class ClickstreamProducer {
	private static final Logger logger = LoggerFactory.getLogger(ClickstreamProducer.class);

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

    for (int i = 1; i <= 10; i++) {
      String clickstreamJSON = ClickStreamGenerator.getClickstreamAsJSON();
      ClickstreamData clickstream =
          gson.fromJson(clickstreamJSON, ClickstreamData.class);
      
      String key = clickstream.domain;
      String value = clickstreamJSON;  
      
      ProducerRecord<String, String> record =
          new ProducerRecord<>(MyConfig.TOPIC_CLICKSTREAM, key,  value);
      logger.debug("sending record # " + i + " : " + record);
      producer.send(record);

    }

    producer.close();

  }

}

package x.lab09_metrics;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.google.gson.Gson;

import x.utils.ClickStreamGenerator;
import x.utils.ClickstreamData;
import x.utils.MyConfig;
import x.utils.MyMetricsRegistry;
import x.utils.MyUtils;

public class ProducerWithMetrics {
	private static final Logger logger = LoggerFactory.getLogger(ProducerWithMetrics.class);
	
	// TODO-1 : get a meter with name 'producer.events'
   private static final Meter meterProducerEvents = MyMetricsRegistry.metrics.meter("???");
	

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
 // "bootstrap.servers" = "localhost:9092"
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MyConfig.DEFAULT_BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "ClickstreamProducer"); // client.id
    props.put("key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer<String, String> producer = new KafkaProducer<>(props);

    Gson gson = new Gson();

    boolean keepRunning = true;
    long eventsSent = 0;
    while (keepRunning) {
      String clickstreamJSON = ClickStreamGenerator.getClickstreamAsJSON();
      ClickstreamData clickstream =
          gson.fromJson(clickstreamJSON, ClickstreamData.class);
      String key = clickstream.domain;
      ProducerRecord<String, String> record =
          new ProducerRecord<>(MyConfig.TOPIC_CLICKSTREAM, key, clickstreamJSON);
      eventsSent++;
      logger.debug("sending event# " + eventsSent + " : " + record);
      producer.send(record);
      
      // TODO-2 : record sending the event, call mark() on the meter
      // meterProducerEvents.??? 
      
      MyUtils.randomDelay(100, 500);

    }

    producer.close();

  }

}

package x.lab_6;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.gson.Gson;

import x.utils.Clickstream;

public class ClickstreamConsumer {

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "group1");
    props.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList(ClickstreamProducer.TOPIC)); // subscribe
                                                                  // to topics

    Gson gson = new Gson();
    boolean keepRunning = true;
    while (keepRunning) {
      ConsumerRecords<String, String> records = consumer.poll(1000);
      // System.out.println("Got " + records.count() + " messages");
      for (ConsumerRecord<String, String> record : records) {
        System.out.println("Received message : " + record);
        
        // TODO-1 : extract the JSON string from record
        // Hint : record.value()
        String clickstreamJSON = "";
        
        // TODO-2 : parse JSON string using GSON library
        // Hint : take a look at ClickStreamProducer around line 30
        Clickstream clickstream = null;

        // TODO-3 : extract the domain attribute
        String domain = "???";
        
        // TODO-1  figure out a way to count domains
        // Hint : Use a HashMap
        
        /* Hint to print a hashmap use this:
         *  System.out.println("Domain COunt is \n"
            + Arrays.toString(map.entrySet().toArray()));
         * 
         */

      }
    }
    consumer.close();
  }
}

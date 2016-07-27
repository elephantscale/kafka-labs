package x.lab_5;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class GreetingsProducer {
  public static final String TOPIC = "greetings";

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("client.id", "GreetingsProducer");
    props.put("key.serializer",
        "org.apache.kafka.common.serialization.IntegerSerializer");
    props.put("value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

    //SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");

    for (int i = 1; i <= 10; i++) {
      Integer key = new Integer(i);
      //String value = df.format(new Date())+  "--" + i + ", Hello world";
      String value =  "Hello world";
      ProducerRecord<Integer, String> record =
          new ProducerRecord<>(TOPIC, key, value);
      System.out.println("sending : " + record);
      producer.send(record);
    }
    producer.close();

  }

}

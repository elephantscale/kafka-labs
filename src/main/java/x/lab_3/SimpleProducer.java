package x.lab_3;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SimpleProducer {

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("client.id", "SimpleProducer");
    props.put("key.serializer",
        "org.apache.kafka.common.serialization.IntegerSerializer");
    props.put("value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

    String topic = "test";
    Integer key = new Integer(1);
    String value = "Hello world";
    ProducerRecord<Integer, String> record =
        new ProducerRecord<>(topic, key, value);
    // option 1 : fire and forget
    System.out.println("sending : " + record);
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

  @Override
  public void onCompletion(RecordMetadata meta, Exception ex) {
    if (ex != null) // error
      ex.printStackTrace();

    if (meta != null) // success
      System.out.println("send success");
  }
}

package x.lab08_avro;
/*
package x.lab8_avro;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.errors.SerializationException;


import java.util.Collections;
import java.util.Properties;

import x.utils.ClickStreamGenerator;
import x.utils.ClickstreamData;

public class AvroClickstreamProducer {

    private static final String TOPIC = "avroclicks";

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) {

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        try (KafkaProducer<String, Clickinfo> producer = new KafkaProducer<String, Clickinfo>(props)) {
        	
            for (long i = 0; i < 10; i++) {
            	ClickstreamData record = ClickStreamGenerator.getClickstreamRecord();
                final long timestamp = record.timestamp;
                final String session =  record.session;
                final String userid = record.user;
                final String domain = record.domain;
                final long cost = record.cost;
                final String campaign = record.campaign;
                final String ip_info = record.ip;
                final Clickinfo clickdata = new Clickinfo(timestamp, session,domain,userid,cost,campaign,ip_info);
                final ProducerRecord<String, Clickinfo> clickrecord = new ProducerRecord<String, Clickinfo>(TOPIC, session, clickdata);
                producer.send(clickrecord);
                Thread.sleep(1000L);
            }

            producer.flush();
            System.out.printf("Successfully produced 10 messages to a topic called %s%n", TOPIC);

        } catch (final SerializationException e) {
            e.printStackTrace();
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }

    }

}
*/

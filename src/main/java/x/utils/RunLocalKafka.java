package x.utils;

import java.nio.file.Files;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;

public class RunLocalKafka {
  private static final String ZKHOST = "127.0.0.1";
  private static final int ZKPORT = 2181;
  private static final String KAFKA_HOST = "127.0.0.1";
  private static final String KAFKA_PORT = "9092";
  private static final String TOPIC = "test";

  public static void main(String[] args) throws Exception {
    //test2();
    test1();
    
  }
  public static void test1() throws Exception {
    try {
      Properties zkProperties = new Properties();
      //zkProperties.setProperty("dataDir", "/tmp/zookeeper");
      zkProperties.setProperty("dataDir", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
      zkProperties.setProperty("clientPort", "" + ZKPORT);

      Properties kafkaProperties = new Properties();
      kafkaProperties.setProperty("zookeeper.connect", ZKHOST + ":" + ZKPORT);
      kafkaProperties.setProperty("broker.id", "0");
      kafkaProperties.setProperty("log.dirs", "/tmp/kafka-logs");
      //kafkaProperties.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());

      kafkaProperties.setProperty("listeners",
          "PLAINTEXT://" + KAFKA_HOST + ":" + KAFKA_PORT);
      
      KafkaLocal kafka = new KafkaLocal(kafkaProperties, zkProperties);
      Thread.sleep(5000);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
  
  public static void test2 () throws Exception {
   
  }
}

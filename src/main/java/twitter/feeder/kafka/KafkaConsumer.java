package twitter.feeder.kafka;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import com.google.common.collect.ImmutableMap;

/**
 * Kafka Consumer.
 * @author fuji-151a
 *
 */
public class KafkaConsumer {
    /** consumer. */
    private ConsumerConnector consumer;
    /** topic name. */
    private String topic;
    /** partition num. */
    private int partition;

    /**
     * 空のConsumer.
     */
    public KafkaConsumer() {
    }
    /**
     * ConsumerConf.
     * @param fileName : propertiesFile
     */
    public KafkaConsumer(final String fileName) {
        Properties prop = new Properties();
            try {
                prop.load(new FileInputStream(fileName));
                if (propFileCheck(prop)) {
                    topic = prop.getProperty("topic");
                    partition = Integer.valueOf(prop.getProperty("partition"));
                } else {
                    System.out.println(prop.getProperty("group.id"));
                    System.out.println(prop.getProperty("zookeeper.connect"));
                    System.out.println(prop.getProperty("topic"));
                    System.out.println(prop.getProperty("partition"));
                    System.out.println("Error");
                    System.exit(1);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println("Input Error");
                e.printStackTrace();
            }
        ConsumerConfig config = new ConsumerConfig(prop);
        consumer = Consumer.createJavaConsumerConnector(config);
    }

    /**
     * Consumer.
     * @return List型のKafkaStream.
     */
    public final List<KafkaStream<byte[], byte[]>> consume() {
        Map<String, Integer> topicCountMap = ImmutableMap.of(topic, partition);
        return consumer.createMessageStreams(topicCountMap).get(topic);
    }

    /**
     * propertiesFile Check.
     * @param prop : properties file
     * @return boolean
     */
    private boolean propFileCheck(final Properties prop) {
        if (prop.getProperty("group.id") == null
                && prop.getProperty("zookeeper.connect") == null
                && prop.getProperty("topic") == null
                && prop.getProperty("partition") == null) {
            return false;
        }
        return true;
    }

}

package twitter.feeder.kafka;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 *
 * @author yuya
 *
 */
public class KafkaProducer {

    /** producer. */
    private Producer<String, String> producer;
    /** Topic Name. */
    private String topic;

    /**
     * 空のコンストラクタ.今のところ未定.
     */
    public KafkaProducer() {
    }

    /**
     * コンストラクタ.Kafkaの設定ファイルを読み込む.
     * @param fileName : propertiesファイル名.
     */
    public KafkaProducer(final String fileName) {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(fileName));
            ProducerConfig config = new ProducerConfig(prop);
            propFileCheck(prop);
            producer = new Producer<String, String>(config);
            topic = prop.getProperty("topic");
        } catch (FileNotFoundException e) {
            System.out.println("FileNotExist");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("Input Error");
            e.printStackTrace();
        }
    }

    /**
     * propertiesFile Check.
     * @param prop : properties file
     * @return boolean
     */
    private boolean propFileCheck(final Properties prop) {
        if (prop.getProperty("topic") == null
                && prop.getProperty("zk.connect") == null
                && prop.getProperty("metadata.broker.list") == null
                && prop.getProperty("serializer.class") == null) {
            System.out.println("---propertiesFile----");
            System.out.println("zk.connect=zookeeperHost:Port");
            System.out.println("topic=TopicName");
            System.out.println("metadata.broker.list=brokerHost:Port");
            System.out.println("serializer.class="
                    + "kafka.serializer.StringEncoder");
            return false;
        } else {
            return true;
        }
    }

    /**
     * produceする機能.
     * @param logData : 流したいデータ
     */
    public final void produce(final String logData) {
        KeyedMessage<String, String> data
            = new KeyedMessage<String, String>(topic, logData);
        producer.send(data);
    }
}

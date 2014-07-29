package kafka.api;

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
    /** zookeeper host and port. */
    private String zk;
    /** brokerhost and port. */
    private String broker;
    /** serializer class. */
    private String serializer;
    /** sleep time. */
    private long sleep = 0;

    /**
     * コンストラクタ.
     * @param topic トピック名
     */
    public KafkaProducer(final String topic,
                        final String zk,
                        final String broker,
                        final String serializer) {
        this.zk = zk;
        this.broker = broker;
        this.topic = topic;
        this.serializer = serializer;
    }

    public final void setKafkaConf() {
        Properties prop = new Properties();
        prop.setProperty("topic", topic);
        prop.setProperty("zk.connect", zk);
        prop.setProperty("metadata.broker.list", broker);
        prop.setProperty("serializer.class", "kafka.serializer.StringEncoder");
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

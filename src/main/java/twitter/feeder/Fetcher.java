package twitter.feeder;

import java.io.UnsupportedEncodingException;
import java.util.List;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import twitter.feeder.kafka.KafkaConsumer;

/**
 * Twitter Fetcher.
 * @author yuya
 *
 */
public class Fetcher {

    /**
     * KafkaからTwitterのデータをFetchする.
     * @param args 1:confFile
     * @throws UnsupportedEncodingException : 指定された文字セットがサポートされてない場合
     */
    public static void main(final String[] args)
            throws UnsupportedEncodingException {
        if (args.length < 1) {
            System.out.println("Please set ConfFile");
            System.exit(1);
        }
        KafkaConsumer kc = new KafkaConsumer(args[0]);
        List<KafkaStream<byte[], byte[]>> streams = kc.consume();
        for (KafkaStream<byte[], byte[]> stream : streams) {
            for (MessageAndMetadata<byte[], byte[]> msg : stream) {
                String data = new String(msg.message(), "UTF-8");
                System.out.println(data);
            }
        }
    }

}

package twitter.feeder;

import twitter.feeder.kafka.KafkaProducer;
import twitter.feeder.stream.Listener;
import twitter.feeder.stream.TwitterStreaming;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Mainクラス.
 * @author yuya
 *
 */
public class TwitterFeederMain {

    /**
     * Main.
     * @param args 0:confFile
     * @throws Exception Fileが開けないければ発生.
     */
    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Please set ConfFile");
            System.exit(1);
        }
        TwitterStreaming twitter = new TwitterStreaming(args[0]);
        twitter.streamSetup();
        twitter.streamRun();
    }

}

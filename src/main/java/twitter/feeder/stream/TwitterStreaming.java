package twitter.feeder.stream;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

/**
 * TwtitterStreamingApi.
 * @author yuya
 *
 */
public class TwitterStreaming {
    /** consumerKey. */
    private static String consumerKey;
    /** cosumerSecret. */
    private static String consumerSecret;
    /** accessToken 初期値. */
    private static String accessToken = null;
    /** accessTokenSecret 初期値. */
    private static String accessTokenSecret = null;
    /** json形式で保存するか. */
    private static boolean jsonStoreEnabled;

    /** ConfFile. */
    private final String confFile;
    /** TwitterStream. */
    private TwitterStream twitterStream;

    /**
     * SetUp API Key.
     * @param fileName PropertiesFile
     * @throws Exception 読み込み失敗時.
     */
    public TwitterStreaming(final String fileName)
            throws Exception {
        this.confFile = fileName;
        PropertiesConfiguration pc = new PropertiesConfiguration(fileName);

        consumerKey = pc.getString("oauth.consumerKey");
        consumerSecret = pc.getString("oauth.consumerSecret");
        accessToken = pc.getString("oauth.accessToken");
        accessTokenSecret = pc.getString("oauth.accessTokenSecret");
        jsonStoreEnabled = pc.getBoolean("jsonStoreEnabled");
    }

    /**
     * streamのセットアップ.
     */
    public void streamSetup() {
        // SetUp AccessToken
        Configuration conf = new ConfigurationBuilder()
            .setOAuthConsumerKey(consumerKey)
            .setOAuthConsumerSecret(consumerSecret)
            .setOAuthAccessToken(accessToken)
            .setOAuthAccessTokenSecret(accessTokenSecret)
            .setJSONStoreEnabled(jsonStoreEnabled)
            .build();
        // TwitterStreamのインスタンス作成
        TwitterStreamFactory factory = new TwitterStreamFactory(conf);
        twitterStream = factory.getInstance();
        // Listenerを登録
        twitterStream.addListener(new Listener(confFile));
    }

    /**
     * streamの実行.
     */
    public void streamRun() {
        twitterStream.sample();
    }
}
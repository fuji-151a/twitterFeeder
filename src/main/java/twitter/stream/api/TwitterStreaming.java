package twitter.stream.api;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

/**
 * TwtitterStreamingApi.
 * @author yuya
 *
 */
class TwitterStreaming {
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
    
    private TwitterStream twitterStream;

    /**
     * SetUp API Key.
     * @param fileName PropertiesFile
     * @throws IOException
     */
    TwitterStreaming(final String fileName)
            throws IOException {
        Properties prop = new Properties();
        prop.load(new FileInputStream(fileName));
        consumerKey = prop.getProperty("oauth.consumerKey");
        consumerSecret = prop.getProperty("oauth.consumerSecret");
        accessToken = prop.getProperty("oauth.accessToken");
        accessTokenSecret = prop.getProperty("oauth.accessTokenSecret");
        jsonStoreEnabled = Boolean.valueOf(prop.getProperty("jsonStoreEnabled"));
        
    }
    
    public static void main(String[] args) throws Exception {
    	TwitterStreaming twitter = new TwitterStreaming("src/main/resources/twitter4j.properties");
		twitter.streamSetup();
		twitter.streamRun();
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
        twitterStream.addListener(new Listener());
    }
    
    /**
     * streamの実行.
     */
    public void streamRun() {
    	twitterStream.sample();
    }
}
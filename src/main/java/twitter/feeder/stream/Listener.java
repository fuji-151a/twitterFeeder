package twitter.feeder.stream;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import twitter.feeder.kafka.KafkaProducer;
import twitter4j.Status;
import twitter4j.StatusAdapter;
import twitter4j.json.DataObjectFactory;

/** Tweetを出力するだけのListener. */
public class Listener extends StatusAdapter {

    /** FIlE NAME. */
    private final String fileName;

    /** Kafka Instnce. */
    private KafkaProducer kp;

    /**
     * コンストラクタ.
     * @param fileName : confFilePath
     */
    public Listener(final String fileName) {
        this.fileName = fileName;
        kp = new KafkaProducer(this.fileName);
    }

    // Tweetを受け取るたびにこのメソッドが呼び出される
    @Override
    public void onStatus(Status status) {
        if (isJapanese(status.getText())) {
            kp.produce(DataObjectFactory.getRawJSON(status));
        }
    }

    /**
     * 日本語のみのフィルタリングを行う.
     * @param text : tweet
     * @return boolean
     */
    private boolean isJapanese(final String text) {
        Matcher m = Pattern.compile("([\\p{InHiragana}\\p{InKatakana}])")
                .matcher(text);
        return m.find();
    }

    /**
     *  ツイートに改行が含まれていた場合は半角空白文字に置き換える.
     * @param s : tweet
     * @return Replace String
     */
    private static String normalizeText(String s) {
          // to space
          s = s.replaceAll("\r\n", "\n");
          s = s.replaceAll("\n", " ");
          return s;
    }
}
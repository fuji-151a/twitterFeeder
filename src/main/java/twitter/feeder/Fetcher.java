package twitter.feeder;

import java.io.File;
import java.io.FileWriter;
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

    /** output fileName. */
    private String fileName;

    /** 改行コード. */
    private static String BR = System.getProperty("line.separator");
    /**
     * KafkaからTwitterのデータをFetchする.
     * @param args 1:confFile
     * @throws Exception FileIO
     */
    public static void main(final String[] args)
            throws Exception {
        if (args.length < 1) {
            System.out.println("Please set ConfFile");
            System.exit(1);
        }
        KafkaConsumer kc = new KafkaConsumer(args[0]);
        Fetcher f = new Fetcher();
        f.setFileName(args[1]);
        List<KafkaStream<byte[], byte[]>> streams = kc.consume();
        for (KafkaStream<byte[], byte[]> stream : streams) {
            for (MessageAndMetadata<byte[], byte[]> msg : stream) {
                String data = new String(msg.message(), "UTF-8");
                f.write(data);
            }
        }
    }

    /**
     * file書き込み.
     * @param msg 書き込みデータ.
     * @throws Exception ファイルの読み込み失敗.
     */
    private void write(final String msg) throws Exception {
        File file = new File(fileName);
        file.createNewFile();
        if (checkBeforeWriteFile(file)) {
            FileWriter fw = new FileWriter(file, true);
            fw.write(msg + BR);
            fw.close();
        }
    }

    /**
     * ファイルの存在とファイル書き込み，ファイル華道家の確かめ.
     * @param file file.
     * @return boolean
     */
    private boolean checkBeforeWriteFile(final File file) {
        if (file.exists()) {
            if (file.isFile() && file.canWrite()) {
                return true;
            }
        }
        return false;
    }

    /**
     * fileNameのSetter.
     * @param fName fileの名前.
     */
    private void setFileName(final String fName) {
        this.fileName = fName;
    }
}

package twitter.feeder;

import java.io.File;
import java.io.FileWriter;
import java.util.Date;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import twitter.feeder.kafka.KafkaConsumer;
import twitter4j.internal.org.json.JSONArray;
import twitter4j.internal.org.json.JSONException;
import twitter4j.internal.org.json.JSONObject;

/**
 * Twitter Fetcher.
 * @author yuya
 *
 */
public class Fetcher {

    /** output fileName. */
    private String fileName;

    /** output dir root path. */
    private String rootPath;

    /** output dir path. */
    private String outputDir;

    /** String dateDir. */
    private String dateDir;

    /** File Sizeの閾値. */
//    private static final long THRESHOLD = 104857600;
    private static final long THRESHOLD = 10485760;

    /** 改行コード. */
    private static String BR = System.getProperty("line.separator");

    /**
     * KafkaからTwitterのデータをFetchする.
     * @param args 1:confFile
     * @throws Exception FileIO
     */
    public static void main(final String[] args)
            throws Exception {
        if (args.length < 2) {
            System.out.println("Please set ConfFile");
            System.out.println("Please set outputdir");
            System.exit(1);
        }
        KafkaConsumer kc = new KafkaConsumer(args[0]);
        Fetcher f = new Fetcher();
        List<KafkaStream<byte[], byte[]>> streams = kc.consume();
        boolean flag = true;
        f.setOutputRootDir(args[1]);
        f.setFileName(f.getToday() + ".json");
        for (KafkaStream<byte[], byte[]> stream : streams) {
            for (MessageAndMetadata<byte[], byte[]> msg : stream) {
                if (flag) {
                    JSONObject jsonObject =
                            new JSONObject(new String(msg.message(), "UTF-8"));
                    String createAt =
                            f.convertDate(jsonObject.getString("created_at"));
                    f.setFileName(createAt + ".json");
                    f.makeDir(createAt);
                    flag = false;
                }
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
        String output = outputDir + fileName;
        File file = new File(output);
        file.createNewFile();
        if (checkBeforeWriteFile(file)) {
            JSONObject jsonObject = new JSONObject(msg);
            String date = convertDate(jsonObject.getString("created_at"));
            if (file.length() > THRESHOLD) {
                this.fileName = date + ".json";
                return;
            } else if (!dateDir.equals(date.substring(0, 8))) {
                makeDir(date);
                return;
            }
            FileWriter fw = new FileWriter(file, true);
            fw.write(msg + BR);
            fw.close();
        }
    }

    /**
     * ファイルの存在とファイル書き込み，ファイルかどうかの確かめ.
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
     * File NameのSetter.
     * @param fName FileName.
     */
    private void setFileName(final String fName) {
        this.fileName = fName;
    }

    /**
     * OutputDir root のSetter.
     * @param dir outputDir.
     */
    private void setOutputRootDir(final String dir) {
        this.rootPath = dir;
    }

    /**
     * outputDirのsetter.
     * @param dir outputdir
     */
    private void setOutputDir(final String dir) {
        this.outputDir = dir;
    }
    
    private void setDateDir(final String dir) {
        this.dateDir = dir;
    }

    /**
     * 本日の日付をyyyyMMddHHmmの形式で作成.
     * @return 本日の日付.
     */
    private String getToday() {
        Date date = new Date();
        DateTime dt = new DateTime(date);
        return dt.toString("yyyyMMddHHmm");
    }

    /**
     * 指定した日付フォーマットに変換.
     * @param date created_atの値
     * @return yyyyMMddHHmmの形式
     */
    private String convertDate(final String date) {
        String[] createdAt = date.split(" ");
        String postDate = createdAt[1] + " "
                + createdAt[2] + ", "
                + createdAt[5] + " "
                + createdAt[3];
        Date d = new Date(postDate);
        DateTime dt = new DateTime(d);
        return dt.toString("yyyyMMddHHmm");
    }

    /**
     * Directoryの作成.
     * @param date twitterデータの日付
     */
    private void makeDir(final String date) {
        String outputPath = rootPath + date.substring(0, 8) + "/";
        File dir = new File(outputPath);
        if (!dir.exists()) {
            dir.mkdirs();
            setDateDir(date.substring(0, 8));
            setOutputDir(outputPath);
        }
    }
}

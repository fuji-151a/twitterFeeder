package twitter.stream.api;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class TwitterStreamingTest {

	/** TestConfファイルのファイル名. */
	private String fileName = "twitter4j.properties";
	
	/** source + fileNameのパス. */
	private String filePath;
	
	@Before
	public void setUp() {
		filePath = TwitterStreaming.class
				.getClassLoader().getResource(fileName).getPath();
	}
	
	public void test() throws Exception {
		TwitterStreaming twitter = new TwitterStreaming(filePath);
		twitter.streamSetup();
		twitter.streamRun();
	}

}

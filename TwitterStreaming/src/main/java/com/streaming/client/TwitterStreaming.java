/**
 * 
 */
package com.streaming.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;

/**
 * @author pavithran ramachandran
 *
 */
public class TwitterStreaming {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub.

		Properties prop = new Properties();
		InputStream input = null;

		input = new FileInputStream("config.properties");

		// load a properties file
		prop.load(input);

		// get the property value and print it out
		String consumerKey = prop.getProperty("consumerKey");
		String consumerSecret = prop.getProperty("consumerSecret");
		String accessToken = prop.getProperty("accessToken");
		String accessTokenSecret = prop.getProperty("accessTokenSecret");
		String filePath = prop.getProperty("filePath");
		String[] filters = new String[] { prop.getProperty("filter") };

		final File file = new File(filePath);

		SparkConf sparkConfig = new SparkConf().setMaster("local[2]").setAppName("Spark Streaming - Twitter");
		// 1 minute streaming window
		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConfig, new Duration(60000));
		System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
		System.setProperty("twitter4j.oauth.accessToken", accessToken);
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

		JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(javaStreamingContext, filters);

		twitterStream.foreachRDD(new Function<JavaRDD<Status>, Void>() {

			@Override
			public Void call(JavaRDD<Status> status) throws Exception {
				// TODO Auto-generated method stub
				status.foreach(new VoidFunction<Status>() {

					@Override
					public void call(Status tweet) throws Exception {
						// TODO Auto-generated method stub
						FileWriter fout = new FileWriter(file, true);
						PrintWriter fileout = new PrintWriter(fout);
						fileout.println("--------------------------------------------------");
						fileout.println(tweet.getCreatedAt());
						fileout.println(tweet.getText());
						fileout.println("--------------------------------------------------");
						fileout.flush();

					}
				});
				return null;
			}
		});

		javaStreamingContext.start();
		javaStreamingContext.awaitTermination();

	}

}

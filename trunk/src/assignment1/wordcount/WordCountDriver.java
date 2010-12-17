package assignment1.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class WordCountDriver {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(assignment1.wordcount.WordCountDriver.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(conf, new Path("input"));
		FileOutputFormat.setOutputPath(conf, new Path("output"));
		
		conf.setMapperClass(assignment1.wordcount.WordCountMapper.class);
		conf.setReducerClass(assignment1.wordcount.WordCountReducer.class);
//		conf.setCombinerClass(assignment1.WordCountReducer.class);
		
		conf.set("io.sort.mb", "10");
		conf.set("mapred.job.tracker", "local");
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
//		String line = "Hello World, Bye World!";
//		StringTokenizer tokenizer = new StringTokenizer(line);
//		while (tokenizer.hasMoreTokens()) {
//			System.out.println(tokenizer.nextToken());
//		}
	}
}

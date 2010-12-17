package assignment1.lineindex;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class LineIndexDriver {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		JobClient client = new JobClient();
		
		JobConf conf = new JobConf(assignment1.lineindex.LineIndexDriver.class);
		conf.setMapperClass(assignment1.lineindex.LineIndexMapper.class);
		conf.setReducerClass(assignment1.lineindex.LineIndexReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.set("io.sort.mb", "10");
		
		FileInputFormat.addInputPath(conf, new Path("indexinput"));
		FileOutputFormat.setOutputPath(conf, new Path("indexoutput"));
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

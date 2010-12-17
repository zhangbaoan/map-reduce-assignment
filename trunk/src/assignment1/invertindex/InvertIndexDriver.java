package assignment1.invertindex;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class InvertIndexDriver {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		JobClient client = new JobClient();
		
		JobConf conf = new JobConf(assignment1.invertindex.InvertIndexDriver.class);
		conf.setMapperClass(assignment1.invertindex.InvertIndexMapper.class);
		conf.setReducerClass(assignment1.invertindex.InvertIndexReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.set("io.sort.mb", "10");
		
		conf.setNumMapTasks(3);
		conf.setNumReduceTasks(3);
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

package pagerank;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class SortDriver {

	public static void deleteDir(String uri) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		Path path = new Path(uri);
		boolean isDeleted = hdfs.delete(path, true);
	}
	
	public static JobConf createJobConf(String iteration_input, String iteration_output) {
		JobConf conf = new JobConf(pagerank.SortDriver.class);
		conf.setMapperClass(pagerank.SortMapper.class);
		conf.setReducerClass(pagerank.SortReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.set("io.sort.mb", "10");

		conf.set("pagerank.dampfactor", String.valueOf(0.85));
		
		FileInputFormat.addInputPath(conf, new Path(iteration_input));
		FileOutputFormat.setOutputPath(conf, new Path(iteration_output));
		
		return conf;
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		JobClient client = new JobClient();
		JobConf conf = createJobConf(args[0], args[1]);
		deleteDir(args[1]);

		client.setConf(conf);
		JobClient.runJob(conf);
	}
}

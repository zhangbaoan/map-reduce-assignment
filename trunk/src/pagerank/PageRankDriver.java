package pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class PageRankDriver {

	public static void deleteDir(String uri) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		Path path = new Path(uri);
		boolean isDeleted = hdfs.delete(path, true);
	}
	
	public static JobConf createJobConf(String iteration_input, String iteration_output) {
		JobConf conf = new JobConf(pagerank.PageRankDriver.class);
		conf.setMapperClass(pagerank.PageRankMapper.class);
		conf.setReducerClass(pagerank.PageRankReducer.class);
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
		String inputDir = args[0];
		String intermediaDir = args[1];
		String outputDir = args[2];
		// set up directory for the iteration
		String iteration_input = intermediaDir;
		String iteration_output = outputDir;
		deleteDir(iteration_output);

		// start the iteration		
		JobClient client = new JobClient();
		int iter_num = 5;
		for (int i = 0; i < iter_num; i++) {
			System.out.println("######################### iteration " + i + " ###############################");
			// create job conf
			System.out.println("******** iteration_input " + iteration_input);
			System.out.println("******** iteration_output " + iteration_output);
			JobConf conf = createJobConf(iteration_input, iteration_output);
			JobClient.runJob(conf);			
			// swap the directories
			String temp = iteration_input;
			iteration_input = iteration_output;
			iteration_output = temp;
			deleteDir(iteration_output);
		}
	}
}

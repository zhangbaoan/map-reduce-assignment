package pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

public class Driver {
	private static double damp_factor = 0.85;
	private static String separator = "###########";
	
	public static void deleteDir(String uri) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		Path path = new Path(uri);
		boolean isDeleted = hdfs.delete(path, true);
	}
	
	public static JobConf createJobConf(String input_dir, String output_dir,
			Class mapper, Class reducer, int reducer_num) {
		JobConf conf = new JobConf(pagerank.PageRankDriver.class);
		conf.setMapperClass(mapper);
		conf.setReducerClass(reducer);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setNumReduceTasks(reducer_num);
		conf.set("io.sort.mb", "10");
		conf.set("pagerank.dampfactor", String.valueOf(damp_factor));

		FileInputFormat.addInputPath(conf, new Path(input_dir));
		FileOutputFormat.setOutputPath(conf, new Path(output_dir));

		return conf;
	}
	
	public static void logInfo2Screen(String info) {
		System.out.println(separator + "\t" + info + "\t" + separator);
	}
}

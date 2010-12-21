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

public class SortDriver extends Driver {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		JobClient client = new JobClient();
		JobConf conf = createJobConf(args[0], args[1],
				pagerank.SortMapper.class, pagerank.SortReducer.class, 1);
		deleteDir(args[1]);

		client.setConf(conf);
		JobClient.runJob(conf);
	}
}

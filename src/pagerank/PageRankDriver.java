package pagerank;

import java.io.IOException;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class PageRankDriver extends Driver {
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
		int iter_num = 10;
		for (int i = 0; i < iter_num; i++) {
			System.out.println("######################### iteration " + i
					+ " ###############################");
			// create job conf
			System.out.println("******** iteration_input " + iteration_input);
			System.out.println("******** iteration_output " + iteration_output);
			JobConf conf = createJobConf(iteration_input, iteration_output,
					pagerank.PageRankMapper.class,
					pagerank.PageRankReducer.class, 1);
			JobClient.runJob(conf);
			// swap the directories
			String temp = iteration_input;
			iteration_input = iteration_output;
			iteration_output = temp;
			deleteDir(iteration_output);
		}
	}
}

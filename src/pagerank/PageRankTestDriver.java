package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class PageRankTestDriver extends Driver {

	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			System.err
					.println("Usage: PageRank <inDir> <outDir> <tempDir> <iters> <reducers>");
			System.exit(1);
		}
		String input_dir = args[0];
		String output_dir = args[1];
		String tempDir = args[2];
		int iters = Integer.parseInt(args[3]);
		int reducers = Integer.parseInt(args[4]);

		// build the link graph
		logInfo2Screen("start building the link graph...");
		String linkgraph_dir = tempDir;
		deleteDir(linkgraph_dir);
		JobConf linkConf = createJobConf(input_dir, linkgraph_dir,
				pagerank.LinkGraphMapper.class,
				pagerank.LinkGraphReducer.class, reducers);
		JobClient.runJob(linkConf);
		logInfo2Screen("done with building the link graph...");

		// apply the iteration
		logInfo2Screen("start the page rank iteration...");
		String iteration_input = linkgraph_dir;
		String iteration_output = output_dir;
		deleteDir(iteration_output);
		for (int i = 0; i < iters; i++) {
			logInfo2Screen("iteration " + i);
			JobConf iterationConf = createJobConf(iteration_input,
					iteration_output, pagerank.PageRankMapper.class,
					pagerank.PageRankReducer.class, reducers);
			JobClient.runJob(iterationConf);
			// swap the directories
			String temp = iteration_input;
			iteration_input = iteration_output;
			iteration_output = temp;
			deleteDir(iteration_output);
		}
		logInfo2Screen("done with the page rank iteration...");

		// sort the result
		logInfo2Screen("start sorting the page rank results...");
		String sort_input = iteration_input;
		String sort_output = iteration_output;
		deleteDir(sort_output);
		JobConf sortConf = createJobConf(sort_input, sort_output,
				pagerank.SortMapper.class, pagerank.SortReducer.class, reducers);
		JobClient.runJob(sortConf);
		logInfo2Screen("done with sorting the page rank results...");

		if (!sort_output.equals(output_dir)) {
			Configuration config = new Configuration();
			FileSystem hdfs = FileSystem.get(config);
			Path srcPath = new Path(sort_output);
			Path dstPath = new Path(output_dir);
			hdfs.copyFromLocalFile(srcPath, dstPath);
		}
		
		// clean the temporality directory
		logInfo2Screen("start cleaning up the temporal directory...");
		deleteDir(tempDir);
		logInfo2Screen("Finish the page rank! Check the result in " + output_dir);
		
		System.exit(0);
	}
}

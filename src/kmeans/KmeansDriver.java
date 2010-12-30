package kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class KmeansDriver extends Driver {

	public static String generateClusters(String filename, int k, String deliminator)
			throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path inFile = new Path(filename);		
		FSDataInputStream in = fs.open(inFile);

		// read the input file
		List<String> strList = new ArrayList<String>();
		String line;
		while ((line = in.readLine()) != null) {
			strList.add(line);
		}

		// generate randomly
		String result = "";
		Random randomGenerator = new Random();
		for (int i = 0; i < k; i++) {
			int randomNum = randomGenerator.nextInt(strList.size() - 1);
			result += strList.get(randomNum) + deliminator;
		}
		return result;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 6) {
			System.err
					.println("Usage: KMeans <inDir> <inFile> <outDir> <tempDir> <iters> <reducers>");
			System.exit(1);
		}
		String input_dir = args[0];
		String filename = args[1];
		String output_dir = args[2];
		String tempDir = args[3];
		int iters = Integer.parseInt(args[4]);
		int reducers = Integer.parseInt(args[5]);
		
		// generate initial clusters randomly
		String deliminator = "\t";
		String clusters = generateClusters(filename, 4, deliminator);
		System.out.println(clusters);
		
		// iteration
		JobConf conf = createJobConf(input_dir, output_dir, clusters,
				kmeans.KmeansMapper.class, kmeans.KmeansReducer.class, reducers);
		JobClient.runJob(conf);
		
		System.exit(0);
	}
}

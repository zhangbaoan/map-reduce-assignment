package kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import kmeans.data.CentersClass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class KmeansDriver extends Driver {

	public static String generateClusters(String uri, int k, String deliminator)
			throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);	
		Path inFile = new Path(uri);
		FileStatus[] status = fs.listStatus(inFile);
		Path[] listedPaths = FileUtil.stat2Paths(status);
		
		// read the input files
		List<String> strList = new ArrayList<String>();
		for (int i = 0; i < listedPaths.length; i++) {
			if (fs.isFile(listedPaths[i])) {
				FSDataInputStream in = fs.open(listedPaths[i]);
				String line;
				while ((line = in.readLine()) != null) {
					strList.add(line);
				}
			}
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

	public static String readClusters(String uri) throws Exception {
		class RegFilter implements PathFilter {
			private String regex = "";
			
			public RegFilter(String regex) {
				this.regex = regex;
			}
			@Override
			public boolean accept(Path path) {
				return !path.toString().matches(regex);
			}
		};
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);	
		Path inFile = new Path(uri);
		FileStatus[] status = fs.listStatus(inFile, new RegFilter(".*\\.crc$"));
		Path[] listedPaths = FileUtil.stat2Paths(status);
		
		// read the input files
		String result = "";
		for (int i = 0; i < listedPaths.length; i++) {
			if (fs.isFile(listedPaths[i])) {
				FSDataInputStream in = fs.open(listedPaths[i]);
				String line;
				while ((line = in.readLine()) != null) {
					result += line;
				}
			}
		}

		return result;
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			System.err
					.println("Usage: KMeans <inDir> <inFile> <outDir> <tempDir> <iters> <reducers>");
			System.exit(1);
		}
		String input_dir = args[0];
		String output_dir = args[1];
		String tempDir = args[2];
		int iters = Integer.parseInt(args[3]);
		int reducers = Integer.parseInt(args[4]);
		
		// generate initial clusters randomly
		String deliminator = "\t";
		int dimensions = 2;
		double epsilon = 1;
		String clusters = generateClusters(input_dir, 4, deliminator);
		System.out.println(clusters);
		
		// iteration
		String iteration_output = output_dir;
		int count = 0;
		while (count < iters) {
			deleteDir(iteration_output);
			JobConf conf = createJobConf(input_dir, iteration_output, clusters,
					kmeans.KmeansMapper.class, kmeans.KmeansReducer.class, reducers);
			JobClient.runJob(conf);
			
			String newclusters = readClusters(iteration_output).replaceAll("\t\t", "\t");			
			CentersClass oldClusters = new CentersClass();
			CentersClass newClusters = new CentersClass();
			oldClusters.setCentersClass(clusters, deliminator, dimensions);
			newClusters.setCentersClass(newclusters, deliminator, dimensions);
			if (oldClusters.dist2Centers(newClusters) < epsilon) {
				logInfo2Screen("stop iteration at " + count);
				break;
			}
			
			logInfo2Screen("error: " + oldClusters.dist2Centers(newClusters));
			
			clusters = newclusters;
			iteration_output = (iteration_output.equals(tempDir)) ? output_dir : tempDir ;
			count++;
		}

//		String dir = "./data/kmeansoutput";
//		System.out.println(readClusters(dir).replaceAll("\t\t", "\t"));
		
		System.exit(0);
	}
}

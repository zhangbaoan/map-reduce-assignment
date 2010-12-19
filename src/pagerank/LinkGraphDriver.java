package pagerank;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class LinkGraphDriver {

	public static void deleteDir(File dir) throws IOException {
		if (!dir.isDirectory()) {
//			throw new IOException("Not a directory " + dir);
			return ;
		}

		File[] files = dir.listFiles();
		for (int i = 0; i < files.length; i++) {
			File file = files[i];

			if (file.isDirectory()) {
				deleteDir(file);
			} else {
				boolean deleted = file.delete();
				if (!deleted) {
					throw new IOException("Unable to delete file" + file);
				}
			}
		}

		dir.delete();
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		JobClient client = new JobClient();

		JobConf conf = new JobConf(pagerank.LinkGraphDriver.class);
		conf.setMapperClass(pagerank.LinkGraphMapper.class);
		conf.setReducerClass(pagerank.LinkGraphReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.set("io.sort.mb", "10");

		conf.set("pagerank.dampfactor", String.valueOf(0.85));

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		File output = new File(args[1]);
		deleteDir(output);

		client.setConf(conf);
		JobClient.runJob(conf);
	}
}

package assignment1.query;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class QueryCombiner extends MapReduceBase implements
	Reducer<Text, Text, Text, Text> {	
	// the snippet for the query
	private Text snippet = new Text();
	// current directory locates in where the jar is
	private String directory = "./indexinput/"; 
	
	/* input key-value pair: <query_string, (name@offset, name@offset, ...)>
	 * Note: reducer can't user reporter to get the path
	 */
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {		
		while (values.hasNext()) {
			String temp = values.next().toString();
			int begin = temp.indexOf("@");
			int end = temp.indexOf(",");
			String filename = temp.substring(0, begin);
			String offset = temp.substring(begin+1, end);
			
			Configuration config = new Configuration();
			FileSystem hdfs = FileSystem.get(config);			
			InputStream in = hdfs.open(new Path(directory+filename));
			in.skip(Long.valueOf(offset));
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String line = reader.readLine();
			
//			RandomAccessFile seeker = new RandomAccessFile(directory+filename, "r");
//			seeker.seek(Long.valueOf(offset));
//			String line = seeker.readLine();
			
			snippet.set(line);
			output.collect(key, snippet);
		}
	}
}

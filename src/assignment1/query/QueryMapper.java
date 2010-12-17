package assignment1.query;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class QueryMapper  extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text>  {	
	private Text queryStr = new Text();
	private Text index_content = new Text();
	
	public void configure(JobConf job) {
		queryStr.set(job.get("query.string"));
	}
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line.toLowerCase());
		String index = tokenizer.nextToken();
		if (index.equals(queryStr.toString())) {
			while (tokenizer.hasMoreTokens()) {
				index_content.set(tokenizer.nextToken());			
				output.collect(queryStr, index_content);
			}
		}
	}
}

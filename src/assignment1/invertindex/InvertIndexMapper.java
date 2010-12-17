package assignment1.invertindex;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class InvertIndexMapper  extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text>  {
	private Text word = new Text();
	private Text index = new Text();
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		FileSplit split = (FileSplit) reporter.getInputSplit();
		String filename = split.getPath().getName();
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line.toLowerCase());
		while (tokenizer.hasMoreTokens()) {
			String temp = tokenizer.nextToken().replaceAll("[^a-zA-Z]", "");
			word.set(temp);
			index.set(filename+"@"+key.toString());
			output.collect(word, index);
		}
	}
}

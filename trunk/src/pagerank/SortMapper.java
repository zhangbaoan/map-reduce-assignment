package pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SortMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {	
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	/* input key: offset
	 * input value: inlink, PageRank, outlink1, ..., outlinkN 
	 * output key: PageRank
	 * output value: inlink
	 */
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		String[] valuesList = line.split("\t");
		if (!valuesList[0].equals("\t")) {
//			System.out.println("#############  " + valuesList[0] + "\t" + valuesList[1]);
			
			outputKey.set(valuesList[1]);
			outputValue.set(valuesList[0]);
			output.collect(outputKey, outputValue);
		}
	}
}

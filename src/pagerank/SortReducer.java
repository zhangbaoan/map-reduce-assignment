package pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SortReducer extends MapReduceBase implements
	Reducer<Text, Text, Text, Text> {
	
	/* input key: PageRank
	 * input value: link
	 * output key: PageRank
	 * output value: link
	 */
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException { 
		Text value = values.next();
//		System.out.println("####### reduce key  " + key.toString());
//		System.out.println("####### reduce value  " + value.toString());
		
		output.collect(key, value);
	}
}

package assignment1.lineindex;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class LineIndexReducer extends MapReduceBase implements
	Reducer<Text, Text, Text, Text> {
	private Text outputValue = new Text();
	
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		StringBuilder sb = new StringBuilder();
		while (values.hasNext()) {
			sb.append(values.next().toString());
		}
		
		outputValue.set(sb.toString());
		output.collect(key, outputValue);
	}
}

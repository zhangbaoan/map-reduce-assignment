package kmeans;

import java.io.IOException;
import java.util.Iterator;

import kmeans.data.DataClass;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class KmeansReducer extends MapReduceBase implements
	Reducer<Text, Text, Text, Text> {
	private Text outputKey = new Text();
	private Text outputValue = new Text("");
	private String separator = "\t";
	
	public void configure(JobConf job) {
		separator = job.get("kmeans.separator");
	}
	
	/* 
	 * input key: center
	 * input value: <data1, data2, ...>
	 * output key: new center
	 * output value: empty
	 */
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException { 
		int count = 1;
		DataClass newCenter = new DataClass(values.next().toString(), separator);
		while (values.hasNext()) {
			DataClass data = new DataClass(values.next().toString(), separator);
			newCenter = newCenter.sum(data);
			count++;
		}
		newCenter = newCenter.divide(count);
		
		outputKey.set(newCenter.toString());
		output.collect(outputKey, outputValue);
	}
}

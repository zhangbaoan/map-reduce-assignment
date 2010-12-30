package kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import kmeans.data.CentersClass;
import kmeans.data.DataClass;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class KmeansMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {	
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	private String separator = "";
	private CentersClass centers = new CentersClass();
	
	public void configure(JobConf job) {
		String strCenters = job.get("kmeans.centers"); // format: f1 \t f2 \t f3 ...
		separator = job.get("kmeans.separator");
		centers.setCentersClass(strCenters, separator, 2);
	}
	
	/* 
	 * input key: offset
	 * input value: data (in the format of coordinate1 coordinate2)
	 * output key: center (in the format of coordinate1 coordinate2)
	 * output value: data (in the format of coordinate1 coordinate2)
	 */
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		DataClass data = new DataClass(line, separator);
		DataClass center = this.centers.whichCenter(data);
		
		outputKey.set(center.toString());
		outputValue.set(data.toString());
		output.collect(outputKey, outputValue);
	}
}

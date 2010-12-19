package pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageRankReducer extends MapReduceBase implements
	Reducer<Text, Text, Text, Text> {
	private double dampFactor; 
	private Text outputValue = new Text();
	private String separator = "\t";
	
	public void configure(JobConf job) {
		dampFactor = Double.valueOf(job.get("pagerank.dampfactor"));
	}
	
	/* input key: link
	 * input value: <PageRank, link1, number of outlinks from link1> ...
	 * or input value: #outlink1, outlink2, ...
	 * output key: link
	 * output value: new PageRank, outlink1, outlink2, ..., outlinkN 
	 */
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException { 
		double sum = 0;
		String outlinks = "";
		while (values.hasNext()) {
			String temp = values.next().toString();
			
//			System.out.println("############  " + temp);
			
			if (temp.indexOf("\t\t") == 0) {
				// it has the outlinks
				outlinks = temp.substring(temp.indexOf("\t\t")+1, temp.length());
//				System.out.println("############ outlinks  " + outlinks);
			}
			else {
				// it has the inlink
				String[] array = temp.split("\t");
//				for (int i = 0; i < array.length; i++) {
//					System.out.println(i + "  " + array[i]);
//				}
				
				sum += Double.valueOf(array[0])/Double.valueOf(array[2]);
			}
		}
		
//		System.out.println("################# sum  " + sum);
//		System.out.println("################# outlinks  " + outlinks);
		
		sum = dampFactor * sum + (1 - dampFactor);
		outputValue.set(String.valueOf(sum) + separator + outlinks);
//		if (key.toString().equals("\"Pussy Cats\" Starring The Walkmen")) {
//			System.out.println("############## key  " + key.toString());
//			System.out.println("############## outputValue  " + outputValue.toString());
//		}

		output.collect(key, outputValue);
	}
}

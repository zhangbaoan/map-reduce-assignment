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

public class PageRankMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {	
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	private String special_separator = "\t\t";
	private String separator = "\t";
	
	private String[] getLinksList(String line) {
		return line.split("\t");
	}
	
	/* input key: offset
	 * input value: inlink, PageRank, outlink1, ..., outlinkN 
	 * output key: outlink i
	 * output value: PageRank, inlink, number of outlinks from inlink
	 * and output key: inlink & output value: #PageRank, outlink1, ..., outlinkN
	 */
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		int separateIndex = line.indexOf("\t");
		String current_link = line.substring(0, separateIndex);
		String rest = line.substring(separateIndex, line.length()); // get the rest of the line
		String[] valuesList = getLinksList(rest); // the array contains the PageRank and the outlinks
		String pagerank = valuesList[1]; // the first element is empty
		
//		System.out.println("##############" + rest);
//		System.out.println("##############current_link " + current_link);
//		System.out.println("##############pagerank " + pagerank);
		
		StringBuilder sb = new StringBuilder(); // save the outlinks
		outputValue.set(pagerank + "\t" + current_link + "\t" + (valuesList.length-1) + "\t");
		for (int i = 2; i < valuesList.length; i++) { // start from the third element since the second is PageRank
			outputKey.set(valuesList[i]);
			output.collect(outputKey, outputValue);
			sb.append(valuesList[i] + separator);
		}
		
		output.collect(new Text(current_link), new Text(special_separator + sb.toString()));
	}
}

package pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class LinkGraphMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {
	private Text docName = new Text();
	private Text linkName = new Text();	
	
	private List<String> getLinksList(String line) {
		List<String> linkslist = new ArrayList<String>();
		
//		Pattern pattern = Pattern.compile("\\[\\[(.*?)\\]\\]");
		Pattern pattern = Pattern.compile("\\<target>(.*?)\\</target>");
		Matcher m = pattern.matcher(line);
		while (m.find()) {
		    String s = m.group(1);
		    if (s.indexOf("|") != -1)
		    	s = s.substring(0, s.indexOf("|"));
		    linkslist.add(s);
		}
		return linkslist;
	}
	
	private String getDocName(String line) {
//		int begin = line.indexOf("<title>") + 7;
//		int end = line.indexOf("</title>");
//		String docName = line.substring(begin, end);
//		return docName;
		
		int beginIndex = line.indexOf("\t");
		int endIndex = line.indexOf("<?");
		if (endIndex == -1) endIndex = line.length();
		
//		System.out.println("############# beginIndex " + beginIndex);
//		System.out.println("############# endIndex " + endIndex);
		
		String substr = line.substring(beginIndex, endIndex).trim();
		substr = substr.substring(0, substr.indexOf("\t"));
		return substr;
	}
	
	/* input key: offset
	 * input value: a line of text
	 * output key: current doc
	 * output value: out link 
	 */
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {		
		String line = value.toString();
		
//		System.out.println("############## line " + line);
		
		String outputKey = getDocName(line);		
		docName.set(outputKey);
		
		List<String> linkslist = getLinksList(line);
		for (int i = 0; i < linkslist.size(); i++) {
			linkName.set(linkslist.get(i));
			output.collect(docName, linkName);
		}
	}
}

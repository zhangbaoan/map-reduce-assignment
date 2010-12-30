package kmeans.data;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Zeng
 * Wrapper for one piece of data
 */
public class DataClass {
	private String separator = "";
	public List<Double> data = new ArrayList<Double>();

	public DataClass(String str, String deliminator) {
		this.separator = deliminator;
		String[] strData = str.split(deliminator);
		for (int i = 0; i < strData.length; i++) {
			data.add(Double.valueOf(strData[i]));
		}
	}
	
	public DataClass(String[] strs, String deliminator) {
		this.separator = deliminator;
		for (int i = 0; i < strs.length; i++) {
			data.add(Double.valueOf(strs[i]));
		}
	}
	
	public double distance(DataClass anotherData) {
		assert (this.data.size() == anotherData.data.size());

		double dist = 0;
		for (int i = 0; i < data.size(); i++) {
			dist += java.lang.Math.pow(
					(this.data.get(i) - anotherData.data.get(i)), 2);
		}
		return dist;
	}
	
	public DataClass sum(DataClass anotherData) {
		assert (this.data.size() == anotherData.data.size());
		
		for (int i = 0; i < this.data.size(); i++) {
			double element = this.data.get(i)+anotherData.data.get(i);
			this.data.set(i, element);
		}
		return this;
	}
	
	public DataClass divide(double num) {
		for (int i = 0; i < this.data.size(); i++) {
			double element = this.data.get(i) / num;
			this.data.set(i, element);
		}
		return this;
	}
	
	public String toString() {
		String output = "";
		Iterator<Double> iter = data.iterator();
		while (iter.hasNext()) {
			output += String.valueOf(iter.next()) + separator;
		}
		return output;
	}
	
	/**
	 * unit test
	 */
	public static void main(String[] args) {
		String clusters = "-19.4\t180.94\t-15.3\t280.94\t-19.4\t180.94\t-19.4\t180.94\t";
		System.out.println(clusters);
		
		CentersClass centers = new CentersClass();
		centers.setCentersClass(clusters, "\t", 2);
		
		System.out.println(centers);
	}
}

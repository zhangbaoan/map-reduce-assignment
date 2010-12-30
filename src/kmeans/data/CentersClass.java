package kmeans.data;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CentersClass {
	private int dimensions = 2;
	private String deliminator = "";
	
	public List<DataClass> centers = new ArrayList<DataClass>();

	/**
	 * @param str
	 *            format: f1 \t f2 \t f3 ...
	 * @param deliminator
	 *            \t
	 */
	public void setCentersClass(String str, String deliminator, int dimensions) {
		this.deliminator = deliminator;
		this.dimensions = dimensions;
		String[] splits = str.split(deliminator);
		String[] array = new String[dimensions];
		for (int i = 0; i < splits.length;) {
			array[0] = splits[i];
			array[1] = splits[i+1];
			DataClass data = new DataClass(array, deliminator);
			centers.add(data);
			i += dimensions;
		}
	}

	public DataClass whichCenter(DataClass data) {
		int min_index = 0;
		double dist = Double.MAX_VALUE;
		for (int i = 0; i < centers.size(); i++) {
			double temp = data.distance(centers.get(i));
			if (temp < dist) {
				dist = temp;
				min_index = i;
			}
		}
		return centers.get(min_index);
	}

	public String toString() {
		String result = "";
		Iterator<DataClass> iter = centers.iterator();
		while (iter.hasNext()) {
			result += iter.next().toString(); 
		}
		return result;
	}
	
	/**
	 * unit test
	 */
	public static void main(String[] args) {
		String clusters = "-19.4\t180.94\t-15.3\t280.94\t-19.4\t180.94\t-19.4\t180.94\t";
		System.out.println(clusters);
		
		CentersClass centers = new CentersClass();
		// test setCentersClass()
		centers.setCentersClass(clusters, "\t", 2);
		// test toString()
		System.out.println(centers);
		// test whichCenter()
		DataClass data = new DataClass("-15\t280", "\t");
		System.out.println(centers.whichCenter(data));
	}
}

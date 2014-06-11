package pagerank;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter; 

public class LoadData_Reducer extends MapReduceBase implements
   Reducer<Text, Text, Text, Text> {

	private static final DecimalFormat df = new DecimalFormat("#.00000");
	private static final double rp_initial = 1.0;
	
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
		while(values.hasNext()){
			sb.append(values.next().toString()+",");
		}
		output.collect(new Text(key.toString()+":"+df.format(rp_initial)), 
				       new Text(sb.toString().substring(0,sb.toString().length()-1)));
	}

	 

}

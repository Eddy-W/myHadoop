package pagerank;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class UpdatePageRank_Mapper extends MapReduceBase implements
		Mapper<Text, Text, Text, Text> {

	private static final DecimalFormat df = new DecimalFormat("#.00000");
	
	public void map(Text key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		String[] vval = value.toString().split(",");
		String[] kval = key.toString().split(":");
		
		double RankN = Double.parseDouble(kval[1]);
		int Nn = vval.length;
		double tmpRP = RankN/Nn;
		String originkey = kval[0];
		for (String val : vval) {
			output.collect(new Text(val), new Text(df.format(tmpRP)));
		}
		output.collect(new Text(originkey), value);	
	}

}

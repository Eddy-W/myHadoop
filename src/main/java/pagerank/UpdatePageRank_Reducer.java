package pagerank;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class UpdatePageRank_Reducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	private static double dampingfactor = 0.15;
	private static final DecimalFormat df = new DecimalFormat("#.00000");
	
	public void configure (Configuration conf){
		dampingfactor=(conf.get("pagerank.dampingfactor")!= null)?Integer.parseInt(conf.get("pagerank.dampingfactor")):0.15;	
	}
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		
		Text keytarget = new Text();
		Text valuetarget = new Text();
		double tmpRP = 0;
		while(values.hasNext()){
			String val = values.next().toString();
			if(val.contains(",")){
				valuetarget.set(val);
			}
			else{
				tmpRP += Double.parseDouble(val);
			}
		}
		double RankK = tmpRP * dampingfactor + (1-dampingfactor);
		keytarget.set(key.toString()+":"+df.format(RankK));
		output.collect(keytarget, valuetarget);
	}

}

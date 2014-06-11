package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf; 
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.InverseMapper;
import org.apache.hadoop.util.Tool;

public class LoadData_MapRed extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		JobConf job = new JobConf(conf);
		job.setJarByClass(LoadData_MapRed.class);
		job.setJobName("pagerank_load_data_phrase");
		
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(InverseMapper.class);
	    job.setReducerClass(LoadData_Reducer.class);
	    
	    job.setInputFormat(KeyValueTextInputFormat.class);
	    job.setOutputFormat(TextOutputFormat.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
		
	    JobClient.runJob(job);
		
		return 0;
	}

}

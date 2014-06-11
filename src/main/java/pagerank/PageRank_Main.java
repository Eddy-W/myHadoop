package pagerank;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public class PageRank_Main extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = new Job();
		job.setJarByClass(PageRank_Main.class);
		job.setJobName("PageRank");
		
		
		return 0;
	}
	public static void main(String[] args) throws Exception {
		
	}

}

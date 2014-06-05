package hppagerank;


import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;


public class pagerankMain {

	public static void main(String[] args) throws Exception {
		
		StopWatch timer = new StopWatch();
		timer.start();

		String dir = "hdfs://localhost:9000/user/hpuser/";
		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		FileSystem fs = FileSystem.get(conf);
		
		int n=10;
		fs.delete(new Path(dir + "pr-"+(n-1)+".out"), true);
		fs.delete(new Path(dir + "prepared.out"), true);

		String[] prepareOpts = { dir + "small_ext1.txt", dir + "prepared.out" };
		ToolRunner.run(new Configuration(), new PrepareTwitterData(), prepareOpts);

		String[] initOpts = { dir + "prepared.out", dir + "pr-0.out" };
		ToolRunner.run(new Configuration(), new InitPageRank(), initOpts);
		
		fs.delete(new Path(dir + "prepared.out"), true);
		
		for(int i = 1; i < n; i++){
			String previous = dir + "pr-" + (i - 1) + ".out";
			String current = dir + "pr-" + i + ".out";
			String[] opts = {previous, current};
			ToolRunner.run(new Configuration(), new UpdatePageRank(), opts);
			fs.delete(new Path(previous), true);
		}
		
		
		timer.stop();
		System.out.println("Elapsed " + timer.toString());

	}

}

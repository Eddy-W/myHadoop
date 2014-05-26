package tfidf;


import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCount extends Configured implements Tool {

	private static final String hdfs = "hdfs://localhost:9000/";
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: ComputeTF <input dir> <output dir>\n");
			return -1;
		}

		Path userInputPath = new Path(args[0]);
		Path userOutputPath = new Path(args[1]);

		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(URI.create(hdfs),conf);

		// Getting the number of documents from the user's input directory.
		// FileStatus[] userFilesStatusList = fs.listStatus(userInputPath);
		final int numberOfUserInputFiles = fs.listStatus(userInputPath).length;

		conf.setInt("numberOfDocsInCorpus", numberOfUserInputFiles);

		Job job = new Job(conf);
		job.setJarByClass(WordCount.class);
		job.setJobName("TF-IDF Job 1: ComputeTF");

	 
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
	 

		FileInputFormat.setInputPaths(job, userInputPath);
		FileOutputFormat.setOutputPath(job, userOutputPath);

		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountCombiner.class);
		job.setReducerClass(WordCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String[] pp=new String[2];
		 pp[0] = "hdfs://localhost:9000/user/hpuser/input";
		 pp[1] = "hdfs://localhost:9000/user/hpuser/output3";
	        
		int exitCode = ToolRunner.run(new Configuration(), new WordCount(), pp);
		System.exit(exitCode);
	}
}

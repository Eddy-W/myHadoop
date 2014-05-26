package tfidf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordInDocFrequency extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: ComputeN <input dir> <output dir>\n");
			return -1;
		}

		Job job = new Job(getConf());
		job.setJarByClass(WordInDocFrequency.class);
		job.setJobName("TF-IDF ComputeN");

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(WordInDocFrequencyMapper.class);
		job.setReducerClass(WordInDocFrequencyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new WordInDocFrequency(), args);
		System.exit(exitCode);
	}
}

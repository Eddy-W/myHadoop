package org.conan.myhadoop.mr;
 

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount   {

	public static class WordCountMapper extends MapReduceBase implements
    Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
						throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
    }
}
}

	public static class WordCountReducer extends MapReduceBase implements
	    Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
		        OutputCollector<Text, IntWritable> output, Reporter reporter)
		        throws IOException {
		    int sum = 0;
		    while (values.hasNext()) {
		        sum += values.next().get();
		    }
		    output.collect(key, new IntWritable(sum));
		}
	}
	  public static JobConf config() {
	      JobConf conf = new JobConf(HdfsDAO.class);
	      conf.setJobName("HdfsDAO");
	      conf.addResource("classpath:/hadoop/core-site.xml");
	      conf.addResource("classpath:/hadoop/hdfs-site.xml");
	      conf.addResource("classpath:/hadoop/mapred-site.xml");
	      return conf;
	  }
    public static void main(String[] args) throws Exception {
        JobConf confx =  config();
        HdfsDAO hdfs = new HdfsDAO(confx);
        hdfs.rmr( "/user/hpuser/output1");
        
        String input = "hdfs://localhost:9000/user/hpuser/input";
        String output = "hdfs://localhost:9000/user/hpuser/output1";

        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("WordCount");
        //conf.addResource("classpath:/hadoop/core-site.xml");
        //conf.addResource("classpath:/hadoop/hdfs-site.xml");
        //conf.addResource("classpath:/hadoop/mapred-site.xml"); 

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(WordCountMapper.class);
        conf.setCombinerClass(WordCountReducer.class);
        conf.setReducerClass(WordCountReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        

         FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

         JobClient.runJob(conf);
         
    }

 

}
package tfidf;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends Mapper<Text, Text, Text, IntWritable> {

	@Override
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {

		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		Path path = fileSplit.getPath();

		String docid = path.getName();

		String lc_line = value.toString().toLowerCase();

		for (String term : lc_line.split("\\W+")) {
			if (term.length() > 0) {
				String termDocId = term + "@" + docid;
				context.write(new Text(termDocId), new IntWritable(1));
			}
		}
	}
}
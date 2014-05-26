package tfidf;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int termCount = 0;

		for (IntWritable value : values) {
			termCount += value.get();
		}

		String outKey = key.toString();

		context.write(new Text(outKey), new IntWritable(termCount));
	}
}
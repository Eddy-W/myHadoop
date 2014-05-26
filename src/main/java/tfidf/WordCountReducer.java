package tfidf;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int docsInCorpus = context.getConfiguration().getInt(
				"numberOfDocsInCorpus", 0);
		
		int termCount = 0;

		for (IntWritable value : values) {
			termCount += value.get();
		}

		String outKey = key.toString() + ":" + docsInCorpus;
		
		context.write(new Text(outKey), new IntWritable(termCount));
	}
}
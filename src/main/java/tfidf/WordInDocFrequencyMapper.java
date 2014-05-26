package tfidf;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordInDocFrequencyMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		String[] keySplit = key.toString().split("@");
		String term = keySplit[0];
		String docid = keySplit[1];

		String val = term + "=" + value.toString();
		context.write(new Text(docid), new Text(val));
	}

}

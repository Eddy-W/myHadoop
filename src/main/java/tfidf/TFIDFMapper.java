package tfidf;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TFIDFMapper extends Mapper<Text, Text, Text, Text> {

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {

		String[] keySplit = key.toString().split("@");
		String term = keySplit[0];
		String docid = keySplit[1].split(":")[0];
		int docsInCorpus = Integer.parseInt(keySplit[1].split(":")[1]);

		context.write(new Text(term + ":" + docsInCorpus), new Text(docid + "=" + value.toString()));
	}
}
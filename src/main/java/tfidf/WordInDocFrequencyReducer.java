package tfidf;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordInDocFrequencyReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int sumOfWordsInDocument = 0;
		Map<String, Integer> tempCounter = new HashMap<String, Integer>();

		for (Text value : values) {
			String[] valueSplit = value.toString().split("=");
			String term = valueSplit[0];
			Integer tf = Integer.valueOf(valueSplit[1]);

			tempCounter.put(term, tf);
			sumOfWordsInDocument += tf;
		}

		for (String term : tempCounter.keySet()) {
			String termInDocId = term + "@" + key.toString();
			String tfWordsInDocId = tempCounter.get(term) + "/"
					+ sumOfWordsInDocument;

			context.write(new Text(termInDocId), new Text(tfWordsInDocId));
		}
	}

}

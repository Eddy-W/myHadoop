package tfidf;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		int docsInCorpusWithTerm = 0;
		Map<String, String> tempFrequencies = new HashMap<String, String>();

		String keySplit[] = key.toString().split(":");
		String term = keySplit[0];
		int docsInCorpus = Integer.parseInt(keySplit[1]);

		for (Text value : values) {

			String[] valueSplit = value.toString().split("=");
			String docId = valueSplit[0];
			String wordCountAndTotalWordsInDoc = valueSplit[1];

			if (Integer.parseInt(valueSplit[1].split("/")[0]) > 0) {
				docsInCorpusWithTerm++;
			}

			tempFrequencies.put(docId, wordCountAndTotalWordsInDoc);
		}

		for (String docid : tempFrequencies.keySet()) {

			String[] wordCountAndTotalWordsInDoc = tempFrequencies.get(docid).split("/");

//			double wordCount = Double.valueOf(wordCountAndTotalWordsInDoc[0]);
//			double totalWordsInDoc = Double.valueOf(wordCountAndTotalWordsInDoc[1]);
//			double tf = wordCount / totalWordsInDoc;
			int wordCount = Integer.parseInt(wordCountAndTotalWordsInDoc[0]);
			int totalWordsInDoc = Integer.parseInt(wordCountAndTotalWordsInDoc[1]);
			double tf = (double) wordCount / (double) totalWordsInDoc;
			double idf = Math.log10((double) docsInCorpus
					/ (double) ((docsInCorpusWithTerm == 0 ? 1 : 0) + docsInCorpusWithTerm));

			double tfIdf = tf * idf;

			DecimalFormat df = new DecimalFormat("0.00000");
			String tfIdfString = df.format(tfIdf);

			String outKey = term + "@" + docid;
			String outValue = wordCount + "/" + totalWordsInDoc + ";" 
					+ docsInCorpusWithTerm + "/" + docsInCorpus + ";"
					+ tfIdfString;

			context.write(new Text(outKey), new Text(outValue));
		}
	}
}

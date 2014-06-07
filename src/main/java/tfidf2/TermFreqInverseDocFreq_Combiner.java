package tfidf2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: deason
 * Date: 7/31/13
 * Time: 9:34 AM
 * To change this template use File | Settings | File Templates.
 */
public class TermFreqInverseDocFreq_Combiner extends Reducer<CompositeKeyForTFIDF, LongWritable, CompositeKeyForTFIDF, LongWritable> {

        public void reduce(CompositeKeyForTFIDF key,
                           Iterator<LongWritable> values,Context context)
                throws IOException, InterruptedException {

            boolean dfEntry = key.getDfEntry();

            if (dfEntry) {
                context.write(key, values.next());
            } else {
                long termFreq = 0;
                while (values.hasNext()) {
                    termFreq += values.next().get();
                }
                context.write(key, new LongWritable(termFreq));
            }
        }
    }

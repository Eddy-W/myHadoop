package tfidf2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.Partitioner;

/**
 * Created with IntelliJ IDEA.
 * User: deason
 * Date: 7/31/13
 * Time: 9:39 AM
 * To change this template use File | Settings | File Templates.
 */
public class TermFreqInverseDocFreq_Partitioner extends Partitioner<CompositeKeyForTFIDF, LongWritable> {

//        public void configure(JobConf job) {}

        public int getPartition(CompositeKeyForTFIDF key, LongWritable value, int numReduceTasks) {
            Text term = key.term;
            return (term.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

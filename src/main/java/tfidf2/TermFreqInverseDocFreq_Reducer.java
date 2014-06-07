package tfidf2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: deason
 * Date: 7/31/13
 * Time: 9:10 AM
 * To change this template use File | Settings | File Templates.
 */
public class TermFreqInverseDocFreq_Reducer extends Reducer<CompositeKeyForTFIDF, LongWritable, Text, DoubleWritable> {

    int numberOfDocuments, docCountForTerm;
    String lastTerm="";

    public void configure(Configuration conf) {
        // Example of using parameters for configuration map/reduce execution
        //Note: This needs to be set as a command line parameter
    	
        numberOfDocuments = (conf.get("tfidf.num.documents") != null) ? Integer.parseInt(conf.get("tfidf.num.documents")) : 1; // set default to 1 for unit testing
        lastTerm = "";
    }

    public void reduce(CompositeKeyForTFIDF key,
    		Iterable<LongWritable> values, 
                       Context context)
            throws IOException, InterruptedException {

    	Iterator<LongWritable>it=values.iterator();
    	
        String docID = key.getDocID();
        String term = key.getTerm();
        boolean dfEntry = key.getDfEntry();

        
        //System.out.println(key); 
        
        
        if (!term.equals(lastTerm)) {
            docCountForTerm = 1;
            lastTerm = term;
        } else {
            if (dfEntry) {
                docCountForTerm++;
               
            } else {
                long termFreq = 0;
                while (it.hasNext()) {
                    termFreq += it.next().get();
                    
                  
                }
                double inverseDocFreq = Math.log((double) numberOfDocuments / docCountForTerm);
                double tfidf = termFreq * inverseDocFreq;
                
                System.out.println(Math.log(1.0));
          // Verbose output for learning purposes. 
                String outTuple = "(" + term + ", " + docID + ")"
                        + " [tf:" + termFreq
                        + " n:" + docCountForTerm
                        + " N:" + numberOfDocuments
                        + "]";
                System.out.println(key);
                context.write(new Text(outTuple), new DoubleWritable(tfidf));
                
            }
        }
        
         
    }
}

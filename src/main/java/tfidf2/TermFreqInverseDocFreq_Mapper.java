package tfidf2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: deason
 * Date: 7/31/13
 * Time: 9:05 AM
 * To change this template use File | Settings | File Templates.
 */
public class TermFreqInverseDocFreq_Mapper extends Mapper<Text, Text, CompositeKeyForTFIDF, LongWritable> {

        public void map(Text key,
                        Text value,
                        org.apache.hadoop.mapreduce.Mapper.Context context)
                throws IOException, InterruptedException {

            LongWritable ONE = new LongWritable(1);

      /* Strip "@..." from key if present. */
            String docID = key.toString();
            int i = docID.indexOf("@");
            if (i != -1) {
                docID = docID.substring(0, i);
            }

            String s = value.toString();
            for (String term : s.split("\\W+")) {
        /* Ignore empty terms and uppercase terms over 1 letter.
           (Added for titles and speakers in Shakespeare.
         */
                if (term.length() > 0 && !((term.length() > 1 ) &  (term.equals(term.toUpperCase())))) {
                    term = term.toLowerCase();
                    context.write(new CompositeKeyForTFIDF(term, docID, true), ONE);
                    context.write(new CompositeKeyForTFIDF(term, docID, false), ONE);
                }
            }
        }
        public static void main(String[] args) throws Exception {
        	Text key=new Text("123@");
        	Text value=new Text("cat cate dog");
        	String docID = key.toString();
        	int i = docID.indexOf("@");
            if (i != -1) {
                docID = docID.substring(0, i);
                System.out.println(docID);
            }
             
            String s = value.toString();
            for (String term : s.split("\\W+")) {
            	if (term.length() > 0 && !((term.length() > 1 ) &  (term.equals(term.toUpperCase())))) {
                    term = term.toLowerCase();
                    System.out.println(new CompositeKeyForTFIDF(term, docID, true));
                    //System.out.println( new LongWritable(1));
                }
            }
        }
    }

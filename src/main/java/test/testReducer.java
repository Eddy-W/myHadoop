package test;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import tfidf2.CompositeKeyForTFIDF;
 

public class testReducer extends Reducer<CompositeKeyForTFIDF, LongWritable, Text, DoubleWritable>{
    protected void reduce(CompositeKeyForTFIDF key, Iterable<LongWritable> values, Context context) 
    		throws java.io.IOException ,InterruptedException {
      
     
    
    context.write(new Text("dd"), new DoubleWritable(0));
    };
}
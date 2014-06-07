package org.conan.myhadoop.mr;

import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class TxtReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
    		throws java.io.IOException ,InterruptedException {
    int sum=0;
    Iterator<IntWritable>it=values.iterator();
    while(it.hasNext()){
    IntWritable value=it.next();
    sum+=value.get();
    }
    context.write(key, new IntWritable(sum));
    };
}
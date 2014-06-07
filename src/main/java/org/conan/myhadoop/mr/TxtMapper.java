package org.conan.myhadoop.mr;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class TxtMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
     protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
    String []strs=value.toString().split(" ");
    for(String str:strs){
    context.write(new Text(str), new IntWritable(1));
    }
     };
}
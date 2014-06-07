package tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;






import tfidf2.CompositeKeyForTFIDF;
import tfidf2.TermFreqInverseDocFreq_Mapper;
import tfidf2.TermFreqInverseDocFreq_Reducer;

import java.util.ArrayList;
import java.util.List;

public class TermFreqInverseDocFreqMRTest {

    
     // Declare harnesses that let you test a mapper, a reducer, and
     // a mapper and a reducer working together.
     
    MapDriver<Text, Text, CompositeKeyForTFIDF, LongWritable> mapDriver;
    ReduceDriver<CompositeKeyForTFIDF, LongWritable, Text, DoubleWritable> reduceDriver;
    MapReduceDriver<Text, Text, CompositeKeyForTFIDF, LongWritable, Text, DoubleWritable> mapReduceDriver;

    
    // Set up the test. This method will be called before every test.
     
    @Before
    public void setUp() {

    
     // Set up the mapper test harness.
     
        TermFreqInverseDocFreq_Mapper mapper = new TermFreqInverseDocFreq_Mapper();
        mapDriver = new MapDriver<Text, Text, CompositeKeyForTFIDF, LongWritable>();
       mapDriver.setMapper(mapper);

    
     // Set up the reducer test harness.
     
        TermFreqInverseDocFreq_Reducer reducer = new TermFreqInverseDocFreq_Reducer();
        Configuration conf = new Configuration(); 
        conf.set("tfidf.num.documents", "2");
        reducer.configure(conf);
        reduceDriver = new ReduceDriver<CompositeKeyForTFIDF,LongWritable, Text, DoubleWritable>();
        reduceDriver.setReducer(reducer);
       

    
     // Set up the mapper/reducer test harness.
     
         mapReduceDriver = new MapReduceDriver<Text, Text, CompositeKeyForTFIDF, LongWritable, Text, DoubleWritable>();
         mapReduceDriver.setMapper(mapper);
         mapReduceDriver.setReducer(reducer);
    }

    
     // Test the mapper.
     
   /*@Test
    public void testMapper() {

    
     // For this test, the mapper's input will be "1 cat cat dog"
     
        mapDriver.withInput(new Text("123"), new Text("cat cate dog"));

    
     // The expected output from mapper phase (given inputs) is "c, 3", "c, 4", and "d, 3".
     
        mapDriver.withOutput(new CompositeKeyForTFIDF("cat","123",true), new LongWritable(1));
        mapDriver.withOutput(new CompositeKeyForTFIDF("cat","123",false), new LongWritable(1));
        mapDriver.withOutput(new CompositeKeyForTFIDF("cate","123",true), new LongWritable(1));
        mapDriver.withOutput(new CompositeKeyForTFIDF("cate","123",false), new LongWritable(1));
        mapDriver.withOutput(new CompositeKeyForTFIDF("dog","123",true), new LongWritable(1));
        mapDriver.withOutput(new CompositeKeyForTFIDF("dog","123",false), new LongWritable(1));
    
     // Run the test.
     
    
        
        mapDriver.runTest();
    }*/

    
      // Test the reducer.
     
   /* @Test
    public void testReducer() {

        List<LongWritable> values = new ArrayList<LongWritable>();
        values.add(new LongWritable(3));
        values.add(new LongWritable(4));

    
     // For this test, the reducer's input will be "c, [3, 4]".
     
        //reduceDriver.withInputKey(new CompositeKeyForTFIDF("c","123",true));
        //reduceDriver.withInputValues(values);
        reduceDriver.withInput(new CompositeKeyForTFIDF("ca","33",true),values)
        .withInput(new CompositeKeyForTFIDF("cd","33",true),values)
        .withInput(new CompositeKeyForTFIDF("cf","33",true),values)
        .withInput(new CompositeKeyForTFIDF("ca","33",false),values);
        
      

    
      //The expected output is "c, 3.5"
     
        reduceDriver.withOutput(new Text("c"), new DoubleWritable(3)); 

    
     //Run the test.
     
        reduceDriver.runTest();
    }*/

    
    // Test the mapper and reducer working together.
     
    @Test
    public void testMapReduce() {

    
     // For this test, the mapper's input will be "1 cat cat dog"
     

        mapReduceDriver.withInput(new Text("1"), new Text("cat cate dog"));
        mapReduceDriver.withInput(new Text("2"), new Text("cat cate dog cat dog"));
    
     // The expected output (from the reducer) is "cat 2", "dog 1".
    

        mapReduceDriver.withOutput(new Text("cat"), new DoubleWritable(3.5));
        mapReduceDriver.withOutput(new Text("cate"), new DoubleWritable(3.0));
    
      //Run the test.
     
        mapReduceDriver.runTest();
    } 
}
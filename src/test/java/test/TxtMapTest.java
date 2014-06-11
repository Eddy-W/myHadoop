package test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import tfidf2.CompositeKeyForTFIDF;
import tfidf2.TermFreqInverseDocFreq_Reducer;


public class TxtMapTest{
private Mapper Map;
private MapDriver driver;
private Reducer reducer;
private ReduceDriver rdriver;
@Before
public void init(){
Map=new TxtMapper();
driver=new MapDriver(Map);
reducer=new testReducer();
rdriver=new ReduceDriver(reducer);
 
}
@SuppressWarnings("unchecked")
@Test
public void testMap()throws Exception{
/*String text="hello world goodbye world hello hadoop goodbye hadoop";
driver.withInput(new LongWritable(), new Text(text))
.withOutput(new Text("hello"),new IntWritable(1))
.withOutput(new Text("world"),new IntWritable(1))
.withOutput(new Text("goodbye"),new IntWritable(1))
.withOutput(new Text("world"),new IntWritable(1))
.withOutput(new Text("hello"),new IntWritable(1))
.withOutput(new Text("hadoop"),new IntWritable(1))
.withOutput(new Text("goodbye"),new IntWritable(1))
.withOutput(new Text("hadoop"),new IntWritable(2)).runTest();*/
    List<LongWritable> values = new ArrayList<LongWritable>();
    values.add(new LongWritable(3));
    values.add(new LongWritable(4));

	rdriver.withInput(new CompositeKeyForTFIDF("hello"), values).withOutput(new Text("dd") , new DoubleWritable(0)).runTest();



}

}
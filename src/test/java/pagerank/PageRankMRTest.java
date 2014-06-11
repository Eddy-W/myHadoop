package pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;

public class PageRankMRTest {

	MapDriver<Text,Text,Text,Text> mapDriver;
	ReduceDriver<Text,Text,Text,Text> reduceDriver;
	MapReduceDriver<Text,Text,Text,Text,Text,Text> mapReduceDriver;
	

	@Before
	 public void setUp() {
		LoadData_Reducer red = new LoadData_Reducer();
		reduceDriver = new ReduceDriver<Text,Text,Text,Text>();
		//reduceDriver.setReducer(red);
		
		
	}

}

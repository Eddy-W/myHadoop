package com.allyes.hive.udf;

import java.util.Random;

import org.apache.hadoop.hive.ql.exec.UDF;

public class RandomValue  extends UDF{
 
	public  Random rnd=new Random(10);
	public String evaluate(Object... args) {
 
		 
		return String.valueOf(rnd.nextDouble());
	}
}

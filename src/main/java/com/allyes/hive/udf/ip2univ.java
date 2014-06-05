package com.allyes.hive.udf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
//import java.nio.charset.Charset;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ip2univ extends UDF{
	 private String fn="/user/group_dataanalysis/wangwentao/ip2univmap.txt"; 
	 //private String fn="/home/wangwentao/Downloads/ip2univmap.txt";
	 //private String fn="ip2univ.txt";
	 private List map=null;
	 public static class Univ implements Comparable{
		 public long startip=0;
		 public long endip=0;
		 public String name="";
		 Univ(long sip,long eip,String nn){
			 this.startip=sip;
			 this.endip=eip;
			 this.name=nn;
		 }
		
		public int compareTo(Object o) {
			// TODO Auto-generated method stub
			Univ tmp = (Univ)o;
			if (tmp.startip>=startip && tmp.endip<=endip)
				return 0;
			else if(tmp.startip<startip)
				return 1;
			else
				return -1;
			 
		}
	 }
	 ip2univ() throws IOException{
		 Configuration conf = new Configuration();
		 FileSystem fs = FileSystem.get(conf);
		 Path inFile = new Path(this.fn); 
		 map=new ArrayList<Univ>();
		 BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(inFile), Charset.forName("UTF-8")));
		 try {
	            String tempString = null;	          	        
	            while ((tempString = reader.readLine()) != null) {
	            	String[] m=tempString.split("\t");
	            	String name=m[0];
			    	Long startip=toNumeric(m[2]);
			    	Long endip=toNumeric(m[3]);	  
				    Univ tt=new Univ(startip,endip,name);
				    this.map.add(tt);
	            }
	            reader.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        } finally {
	            if (reader != null) {
	                try {
	                    reader.close();
	                } catch (IOException e1) {
	                }
	            }
	        }
	        fs=null;
	        reader=null;
	        /*for(int i=0;i<this.map.size();++i){
	        	Univ tmp = (Univ)this.map.get(i);
	        	System.out.println(tmp.startip);
	        
	        }*/

	 }
 
	 static Long toNumeric(String ip) {
	        Scanner sc = new Scanner(ip).useDelimiter("\\.");
	        return 
	            (sc.nextLong() << 24) + 
	            (sc.nextLong() << 16) + 
	            (sc.nextLong() << 8);// + 
	           // (sc.nextLong()); 
	}
	 public String evaluate(Object... args) throws IOException {
		 	 
		  	try
		 	{
		  		Long targetip=toNumeric(args[0].toString());
		  		Univ tt=(Univ)this.map.get(Collections.binarySearch(this.map, new Univ(targetip,targetip,"")));
		  		return tt.name;
		 	}
		 	catch(Exception e){
		 		return null;
		 		}
		  	
			
		 }

		    public static void main(String[] args) throws IOException{
		    	//ip2univ ip=new ip2univ();
		    	//System.out.println(ip.evaluate("223.129.252.863")); 
		    }
}

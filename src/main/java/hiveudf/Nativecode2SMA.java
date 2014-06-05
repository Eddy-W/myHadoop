package hiveudf;

import java.io.File;
import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;

public class Nativecode2SMA extends UDF{
	private String adxCategoryDictFile="adx2sma.txt";
	public Dictionary<String, String> OthelloDict = new Hashtable <String, String>();
	Nativecode2SMA() throws IOException{
		String[] res = readFileByLines(this.adxCategoryDictFile);
		for(int i=0;i<res.length;++i)
		{
			String[] tmp = res[i].split("\t");
			OthelloDict.put(tmp[0], tmp[1]);
		}
	}
	public String evaluate(Object... args) throws IOException {
		
        String columnValue[] = new String[args.length];
    
        if (args[0]==null || args.length!=2)
        	return null;
        for (int i = 0; i < args.length; i++){
            columnValue[i] = args[i].toString();
        }
        
        String res="";
        String[] tmpnativecode = columnValue[0].substring(1,columnValue[0].length()-1).split(",");
        String[] tmpweigh = columnValue[1].substring(1,columnValue[1].length()-1).split(",");
        for(int i=0;i<tmpnativecode.length;++i)
        {
        	res+=transfromcode(tmpnativecode[i])+":"+tmpweigh[i]+",";
        }
        
		return res.substring(0, (res.length()-1));
	}
	public String transfromcode(String code){
		String cat = this.OthelloDict.get(code);
		if(cat==null)
			return "others";
		else
			return cat;
	}
	public static void main(String[] args) throws IOException{
		   Nativecode2SMA dd = new Nativecode2SMA();
		   String re=dd.evaluate("[1545,1546]","[34.23,324,12]");
		   System.out.println(re);
	    	
	}
	/**
     * 以行为单位读取文件，常用于读面向行的格式化文件
	 * @throws IOException 
     */
    public String[] readFileByLines(String fileName) throws IOException {
        File file = new File(fileName);
        
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path inFile = new Path(fileName);
		FSDataInputStream reader =null;
         
		 
		
        int cnt=0;
         
        try {      
            //reader = new BufferedReader(new FileReader(file));
        	reader =  fs.open(inFile);
            while (reader.readLine() != null) {
            	cnt++;
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
        
        String[] res = new String[cnt];
        try {
           
        	//reader = new BufferedReader(new FileReader(file));
        	reader =  fs.open(inFile);
            String tempString = null;
            int line = 0;
             
            while ((tempString = reader.readLine()) != null) {
         
                res[line]=tempString;
                line++;
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
        return res;
    }

}

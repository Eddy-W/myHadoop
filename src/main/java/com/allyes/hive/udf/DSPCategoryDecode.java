package com.allyes.hive.udf;

import java.io.File;
import java.io.IOException;
 









import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;

import com.allyes.hive.udf.TreeUtil.tree;
import com.allyes.hive.udf.TreeUtil.treeNode;
 

 

public class DSPCategoryDecode  extends UDF {

	 
	private String adxCategoryDictFile="/home/wangwentao/Downloads/ad.txt";
	private String hiveFile = "/user/hive/advbaseinfo/winmax/adx_media_category/adx_category_map";
	tree<String> AdxTree=null;
	DSPCategoryDecode() throws IOException{
		buildAdxTree();
	}

	public void buildAdxTree() throws IOException{
		String[] res = readFileByLines(this.adxCategoryDictFile);
		//String[] res = readFileByLines(this.hiveFile);
		this.AdxTree = new tree();
		AdxTree.addNode(null, "0","-1","root");
		for(int i=1;i<res.length;++i){
			String[] tmp = res[i].split("\t");
			String id = tmp[0];
			String pid = tmp[2];
			String name = tmp[1];
			String nativeid = tmp[3];
			//System.out.println(nativeid);
			AdxTree.addNode(AdxTree.getNode_node(pid), id,nativeid,name);
		}
		AdxTree.updateNodelevel(AdxTree.root, -1);
		//AdxTree.updateNodelevel(AdxTree.root, -1);
		AdxTree.showNode(AdxTree.root,-1);
		 
		//AdxTree.showNode(AdxTree.root,1);
		/*for(int i=1;i<res.length;++i){
			String[] tmp = res[i].split("\t");
			String nativeid = tmp[3];
		 
			System.out.println(nativeid+":"+AdxTree.searchbycode(nativeid, "0").Name);
		}*/
		
		
		//System.out.println("OK"); 
	}
	public String evaluate(Object... args) throws IOException {
		
        String columnValue[] = new String[args.length];
        int argsNum = args.length;
        if (args[0]==null)
        	return null;
        for (int i = 0; i < args.length; i++){
            columnValue[i] = args[i].toString();
        }
       
        String res = "";
        if(argsNum==2){
        	String nativecodestr=columnValue[0];
        	String type=columnValue[1];
        	if(type.equals("parent")){
        		String[] nativecodes = nativecodestr.split(",");
        		for(int i=0;i<nativecodes.length;++i){
        			treeNode<String> tmp= this.AdxTree.search2level(nativecodes[i], 1);
        			if(tmp!=null)
        				res+=tmp.Name.toString()+",";
        		}
        	}
        		
        	else if(type.equals("self"))
        	{
        		String[] nativecodes = nativecodestr.split(",");
        		for(int i=0;i<nativecodes.length;++i){
        			treeNode<String> tmp= this.AdxTree.getNode(nativecodes[i]);
        			if(tmp!=null)
        				res+=tmp.Name.toString()+",";
        		}
        	}
        		
        	else
        		return null;
        	//return this.AdxTree.searchbycode(nativecode, "0").Name.toString();
        }
        if (res.length()>0)
        	return res.substring(0, res.length()-1);
        else
        	return null;
		
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
    public static void main(String[] args) throws IOException{
    	DSPCategoryDecode dd = new DSPCategoryDecode();
    	System.out.println(dd.evaluate("1243","self"));
    	
    }
}

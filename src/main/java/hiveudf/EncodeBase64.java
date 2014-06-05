package hiveudf;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import base64.*;

public class EncodeBase64 extends UDF{
	Text result = new Text();
    public EncodeBase64() {
    }
    
    public Text evaluate(String str){
    	if (str == null || str.equals("")) {
    		return null;
    	}
    	try {
    		byte[] b = str.getBytes();
    		result.set(Base64.encodeBase64(b));
    	} catch (Exception e) {
    		e.printStackTrace();
    		return null;
    	}
    	
    	return result;
    }
    public static void main(String[] args) throws IOException{
    	EncodeBase64 x = new EncodeBase64();
    	System.out.println(x.evaluate("93"));
    }
}
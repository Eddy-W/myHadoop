package hiveudf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Url2domain extends UDF {

	 public String evaluate(Object... args) {
		
		if(args[3]!=null)
			return "anonymous";
		/*if(args.length>1)
			return null;
		
		String url=args[0].toString();
		String[] tmp = url.split("\\.");
	 
		if(tmp.length<=2)
			return url;
		else
		{
			String ans="www";
			for(int i=1;i<tmp.length;++i){
				ans+="."+tmp[i];
			}
			return ans;
		}*/
		String res="";
		String tmp="";
		if (args[0].toString().contains("http://"))
			tmp=args[0].toString().replace("http://", "").split("/")[0];
		else
			tmp=args[0].toString().split("/")[0];
		if (args[0].toString().contains("mbapp"))
		{
			if(args[1]!=null){
				if(args[2]!=null)
					res= "mobileapp"+"-"+args[1].toString()+"-"+args[2].toString();
			}
			else
				res= "mobileapp";
		}
		else
		{
			System.out.println(tmp);
			String[] tmpx = tmp.split("\\.");
			if(tmpx.length>2)
			{	
				res="www";
				for(int i=1;i<tmpx.length;++i)
					res+="."+tmpx[i];
			}
			else
				res=tmp;
			
		}
		return res;

		
		 
	 }
	    public static void main(String[] args){
	    	Url2domain url=new Url2domain();
	    	System.out.println(url.evaluate("fashion.ifeng.com/health/longevity/explore/detail_2013_10/21/30499044_0.shtml")); 
	    }
}

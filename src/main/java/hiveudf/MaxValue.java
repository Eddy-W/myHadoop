package hiveudf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class MaxValue extends UDF{
	public String evaluate(Object... args) {
		 
		double res=-1;
		for (int i=0;i<args.length;++i){
			double tmp=Double.parseDouble(args[i].toString());
			if(tmp>res)
				res=tmp;
		}
		return String.valueOf(res);
	}
}

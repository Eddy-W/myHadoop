package hiveudf;
import java.net.URLDecoder;
import java.net.URLEncoder;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


public class URLDecodeUDF extends UDF {
	public Text evaluate(final Text s) {
		if (s == null) {
			return null;
		}
		
		String ret = "";
		try {
			ret = URLDecoder.decode(s.toString(), "UTF-8");
		} catch (Exception e) {}
		return new Text(ret);
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println(URLEncoder.encode("53°茅台飞天500ml 53°汾酒三十年陈酿850ml 53°十年红花郎酒500ml 组合套装", "UTF-8"));
	}
}

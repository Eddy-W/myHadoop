package hiveudf;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class URLParamExtractUDF extends UDF {
	public Text evaluate(Text url, Text key) {
		if (url == null || key == null) {
			return null;
		}
		
		try {
			String[] urlFields = StringUtils.splitPreserveAllTokens(url.toString(), "?");
			String[] params = StringUtils.splitPreserveAllTokens(urlFields[1],
					"&");
			Map<String, String> paramMap = new HashMap<String, String>();
			for (String param : params) {
				//String[] fields = StringUtils
				//		.splitPreserveAllTokens(param, "=");
				int pos = param.indexOf('=');
				String k = param.substring(0, pos);
				String v = param.substring(pos+1);
				paramMap.put(k,v);
			}
			String ret = paramMap.get(key.toString());
			return new Text(ret);
		} catch (Exception e) {
			System.err.println(e);
		}
		return new Text("");
	}
}

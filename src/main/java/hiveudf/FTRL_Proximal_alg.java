package hiveudf;

 
import hiveudf.Top4GroupBy.State;

import java.util.Vector;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

 
public class FTRL_Proximal_alg extends UDAF {

    //定义一个对象用于存储数据
    public static class Parameter {
        private   double[] zi;
        private   double[] ni;
        private   double[] wi;

    }
	
	public static class IterateByEvaluator implements UDAFEvaluator {

		private final Parameter parameters;
		private static double alpha=1;
		private static double beta=1;
		private static double lamba1=1;
		private static double lamba2=1;
		private static int N=5;
		
		public IterateByEvaluator() {
			super(); 
			parameters = new Parameter();
	        init();  
	       }
		

		public boolean iterate(Object... X) {
			double[] x=new double[X.length-1];
            for(int i=0;i<X.length-1;++i)
            {
            	double xi=Double.parseDouble(X[i].toString());
            	x[i]=xi;
            	if(xi>0)
            		updateparameters(xi,i);
            }
            
            double pt=predict(x);
            double yt=Double.parseDouble(X[X.length-1].toString());
            updateparameters(x,pt,yt);
            return true;
        }
		private void updateparameters(double[] x, double pt, double yt) {
			// TODO Auto-generated method stub
			for(int i=0;i<x.length;++i){
				double zi=parameters.zi[i];
				double ni=parameters.ni[i];
				double wi=parameters.wi[i];
				double gi=(pt-yt)*x[i];
				double roi=(Math.sqrt(ni+gi*gi)-Math.sqrt(ni))/alpha;
				zi=zi+gi-roi*wi;
				ni=ni+gi*gi;
				parameters.zi[i]=zi;
				parameters.ni[i]=ni;
				parameters.wi[i]=wi;
				
				
			}
		}
		private double predict(double[] x) {
			// TODO Auto-generated method stub
			double sum=0;
			for(int i=0;i<x.length;++i){
				sum+=x[i]*parameters.wi[i];
			}
			return sigmoid(sum);
		}
		private void updateparameters(double xi,int i) {
			// TODO Auto-generated method stub
			 
				double zi=parameters.zi[i];
				double ni=parameters.ni[i];
				double wi=0;
				if(Math.abs(zi)<=lamba1){
					wi=0;
				}
				else{
					wi=-(zi-sgn(zi)*lamba1)/((beta+Math.sqrt(ni))/(alpha)+lamba2);
				}
				parameters.wi[i]=wi;
			
		}
		
		public static  double sgn(double s){
			 
			if(s>0)
				return 1.0;
			else if(s<0)
				return -1.0;
			else 
				return 0.0;
		}
		public static double sigmoid(double s){
			return 1.0/(1+1/Math.exp(s));
		}

 
		public void init() {
			// TODO Auto-generated method stub
			parameters.zi=new double[N];
			parameters.ni=new double[N];
			parameters.wi=new double[N];
		}
		
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

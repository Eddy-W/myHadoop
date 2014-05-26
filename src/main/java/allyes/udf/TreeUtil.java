package allyes.udf;

import java.util.ArrayList;
import java.util.List;
 

public class TreeUtil {

	public static class treeNode<T> {
	    public T ID;
	    private treeNode<T> parent;
	    
	    public String Name="";
	    public T Nativecode ;
	    public int level=-1;
	    
	    
	    public List<treeNode<T>> nodelist;
	    
	    public treeNode(T stype,T code,String name){
	        ID      = stype;
	        Name = name;
	        Nativecode = code;
	        
	        nodelist = new ArrayList<treeNode<T>>();
	    }

	    public treeNode<T> getParent() {
	        return parent;
	    }    
	}
	
	public  static class tree<T> {
	    
	    public treeNode<T> root;
	    
	    public tree(){}
	        
	    public void addNode(treeNode<T> node, T newNode, T code,String name){
	        //增加根节点
	        if(null == node){
	            if(null == root){
	                root = new treeNode(newNode,code,name);
	            }
	        }else{
	                treeNode<T> temp = new treeNode(newNode,code,name);
	                temp.parent=node;
	                node.nodelist.add(temp);
	                
	        }
	    }
	    
	    /*    查找newNode这个节点 */
	    public treeNode<T> search_at_pl(treeNode<T> input, int pl){
	    
	        
	        
	        if(input.level==pl){
	            return input;
	        }
	        else if (input.level<pl)
	        {
	        	return null;
	        }
	        else{
	        	return search_at_pl(input.getParent(), pl);
	        }
	        
	    }
	    
	    /*    查找newNode这个节点 */
	    public treeNode<T> search(treeNode<T> input, treeNode<T> s){
	    
	        treeNode<T> temp = null;
	        
	        if(input.ID.equals(s.ID)){
	            return input;
	        }
	        
	        for(int i = 0; i < input.nodelist.size(); i++){
	            
	            temp = search(input.nodelist.get(i), s);
	            
	            if(null != temp){
	                break;
	            }    
	        }
	        
	        return temp;
	    }
	    
	    /*    查找newNode这个节点 */
	    public treeNode<T> search_ext(treeNode<T> input, T code){
	    
	        treeNode<T> temp = null;
	        
	        if(input.Nativecode.equals(code)){
	            return input;
	        }
	        
	        for(int i = 0; i < input.nodelist.size(); i++){
	            
	            temp = search_ext(input.nodelist.get(i), code);
	            
	            if(null != temp){
	                break;
	            }    
	        }
	        
	        return temp;
	    }
	 
	    public treeNode<T> search2level(T code,int pl){
	    	if (code==null)
	    		return null;
	    	return search_at_pl(getNode(code),pl);
	    	
	    }
	    
	    public treeNode<T> getNode(T code){
	    	if(code==null)
	    		return null;
	        return search_ext(root, code);
	    }
	    	    public treeNode<T> getNode_node(T newNode){
	        return search(root, newNode);
	    }
	        /*    查找newNode这个节点 */
	    public treeNode<T> search(treeNode<T> input, T newNode){
	    
	        treeNode<T> temp = null;
	        
	        if(input.ID.equals(newNode)){
	            return input;
	        }
	        
	        for(int i = 0; i < input.nodelist.size(); i++){
	            
	            temp = search(input.nodelist.get(i), newNode);
	            
	            if(null != temp){
	                break;
	            }    
	        }
	        
	        return temp;
	    }
	    public void updateNodelevel (treeNode<T> node,int level){
	    	 
	        if(null != node){
	            //循环遍历node的节点

	 
	            node.level=level+1;
	            
	            for(int i = 0; i < node.nodelist.size(); i++){
	            	updateNodelevel(node.nodelist.get(i),level+1);
	            }   
	             
	        }
	    }
	    public void showNode(treeNode<T> node,int level){
	    	//if(level>0)
	    		//return;
	        if(null != node){
	            //循环遍历node的节点
	        	String tmp="";
	        	String tmp2="";
	        	for(int i=0;i<level+1;++i)
	        		tmp+="--";

	 
	            //System.out.println(tmp+node.ID.toString()+":"+node.Nativecode+":"+node.Name+":"+node.level);
	        	if(node.parent!=null)
	        		System.out.println(node.ID.toString()+"\t"+node.Nativecode+"\t"+node.Name+"\t"+search2level(node.Nativecode,1).Name);
	        	else
	        		System.out.println(node.ID.toString()+"\t"+node.Nativecode+"\t"+node.Name+"\t"+node.Name);
	        	//System.out.println(node.Name);
	            for(int i = 0; i < node.nodelist.size(); i++){
	                showNode(node.nodelist.get(i),level+1);
	            }   
	             
	        }
	    }
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

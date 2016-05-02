package simplePageRank;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class getBlockID {
	 //public int[] index = {0,10328,20373,30629,40645,50462,60841,70591,80118,90497,100501,110567,120945,130999,140574,150953,161332,171154,181514,191625,202004,212383,222762,232593,242878,252938,263149,273210,283473,293255,303043,313370,323522,333883,343663,353645,363929,374236,384554,394929,404712,414617,424747,434707,444489,454285,464398,474196,484050,493968,503752,514131,524510,534709,545088,555467,565846,576225,586604,596585,606367,616148,626448,636240,646022,655804,665666,675448,685230}; 
     public static int totalLine = 0;
	 public static void main(String[] args) throws IOException{
	      // Invoke the constructor to setup the GUI, by allocating an instance
		  getBlockID test = new getBlockID();
		  System.out.println(test.blockIDofNode(0));
	   }
	    
	public long blockIDofNode(long nodeID) throws IOException{
    	
    	ArrayList<Long> nodeIndex = new ArrayList<Long>();
    	nodeIndex.add(new Long(0));
        BufferedReader br = new BufferedReader(new FileReader("/Users/jd/Downloads/CS5300-Project2-master/PageRankMapReduce/PreprocessFinalWithDeadNodesStaticPR.txt"));
    	String content = "";
    	long total = 0; 
    	while (content != null) {
    		content = br.readLine();
    		if (content == null) {
    			break;
    		}
//    		for (int i = 0; i < content.length(); i++) {
//    			if (content.charAt(i) != ' ') {
//    				content = content.substring(i);
//    				break;
//    			}
    		totalLine ++;
    		}
            
//    		long node = Long.parseLong(content);
//    		total += node;
//    		nodeIndex.add(total);
//    	}
//    	Object[] index = new Object[nodeIndex.size()];   	
//    	index = nodeIndex.toArray();
//    	String s = "";
//for (int i = 0; i < 69; i++) {
//	s += index[i] + ",";
//}
//System.out.print(s);
//    	int begin = 0;
//    	int last = index.length - 1;
//    	int middle = 0;
//		while (last - begin > 1) {
//			middle = begin + (last - begin) / 2;
//			if (nodeID >= index[middle]) {
//				begin = middle;
//			}
//			if (nodeID < index[middle]) {
//				last = middle;
//			} 			
//		}
//		return begin;
return totalLine;
    }
}

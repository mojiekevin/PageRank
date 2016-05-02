package gaussPageRank;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class gaussPageRank_map extends Mapper<LongWritable, Text, Text, Text>{
	
	
	public static final int[] blocks = {0,10328,20373,30629,40645,50462,60841,70591,80118,90497,100501,110567,120945,130999,140574,150953,
        								161332,171154,181514,191625,202004,212383,222762,232593,242878,252938,263149,273210,283473,293255,
        								303043,313370,323522,333883,343663,353645,363929,374236,384554,394929,404712,414617,424747,434707,
        								444489,454285,464398,474196,484050,493968,503752,514131,524510,534709,545088,555467,565846,576225,
        								586604,596585,606367,616148,626448,636240,646022,655804,665666,675448,685230};
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		String[] parameter = getParameters(value);
		int nodeID = Integer.parseInt(parameter[0]);
		float PageRank = Float.parseFloat(parameter[1]);
		int outDegree = Integer.parseInt(parameter[2]);
		String outgoingLinks = parameter.length == 4? parameter[3]:"";
		int blockID = blockIDofNode(nodeID);
		context.write(new Text(""+blockID), new Text("is_node " + nodeID + " " + PageRank + " " + outgoingLinks));
		
		if (outgoingLinks != "") {	
			String[] outlinks = outgoingLinks.split(",");
			for(String s : outlinks){
				int blockIDofNeighbor = blockIDofNode(Integer.parseInt(s));
				Text valueOfmap = new Text();
				//If the node and its neighbor are in the same block, map this relation.
				if(blockID == blockIDofNeighbor){
					valueOfmap = new Text("same_block " + nodeID + " " + s);
				}
				//If the node and its neighbor are in different blocks, map this relation and the boundary condition.
				else{
					float v = PageRank / outDegree;
					valueOfmap = new Text("differ_block " + nodeID + " " + s + " " + v);
				}
				context.write(new Text(""+blockIDofNeighbor), valueOfmap);
			}
		}
	}
	
	public String[] getParameters(Text value){
		return value.toString().trim().split("\\s+");
	}	
	/**
	 * For a given node ID, find the block it belongs to.
	 * @param nodeID
	 * @return blockId
	 */
	public int blockIDofNode(int nodeID){
		int[] index = blocks;
    	int begin = 0;
    	int last = index.length - 1;
    	int middle = 0;
		while (last - begin > 1) {
			middle = begin + (last - begin) / 2;
			if (nodeID >= index[middle]) {
				begin = middle;
			}
			if (nodeID < index[middle]) {
				last = middle;
			} 			
		}
		return begin;		
	}
	
	/**
	 * Given a block ID, check whether or not it is in 'blocks'
	 */
	public static boolean checkInblocks(int n){
		for(int m : blocks){
			if(n == m){
				return true;
			}
		}
		return false;
	}
	
}
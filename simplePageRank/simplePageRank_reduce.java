package simplePageRank;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class simplePageRank_reduce extends Reducer<Text, Text, Text, Text>{
	
	public static float randomJump = 0.85f;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
				
		float oldPageRank = 0.0f;
		float newPageRank = 0.0f; 
		float sumOfPageRank = 0.0f;
		float residualError = 0.0f;
		String outgoingLinks = "";
		Iterator<Text> iter = values.iterator();
		
		while(iter.hasNext()){
			//Recover graph structure
			String[] valueArray = iter.next().toString().trim().split("\\s+"); 
			if(valueArray[0].equals("is_node")){
				oldPageRank = Float.parseFloat(valueArray[1]);
				outgoingLinks = valueArray.length==3? valueArray[2]:"";
			}
			//Sum incoming PageRank contributions
			else{
				sumOfPageRank += Float.parseFloat(valueArray[0]);
			}
		}
		
		//Update the PageRank value of current node
		newPageRank = simplePageRank_reduce.randomJump*sumOfPageRank+(1-simplePageRank_reduce.randomJump)/simplePageRank.numOfwebGraphNodes;
	
		//Compute the residual_error for the current node
		residualError = Math.abs(newPageRank-oldPageRank) / newPageRank;
	
		//Accumulate error over the whole graph
		context.getCounter(accumulator.counter.RESIDUALERRORS).increment((long)(residualError*100000000));
		
		//Emit node as input of the map
		context.write(key, new Text(newPageRank+" "+outgoingLinks.split(",").length+" "+outgoingLinks));
		
	}

}	
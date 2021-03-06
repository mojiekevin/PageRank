package randomBlockPartition;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class randomBlockPartition_reduce extends Reducer<Text, Text, Text, Text> {

	public static float randomJump = 0.85f;
	public static float stoppingThreshold = 0.001f;
	
	//For each node within a block
	//Store the node and its outgoing links within the same block
	private Map<String,ArrayList<String>> nodeInblock = new HashMap<String,ArrayList<String>>();
	//Store the boundary condition of the node
	private Map<String,Float> boundaryCondition = new HashMap<String,Float>();
	//Keep track of the processed nodes within the block
	private Map<String,webNode> records = new HashMap<String,webNode>();
	//Store updated PageRank value of the node
	private Map<String,Float> updatedPageRank = new HashMap<String,Float>();
	//Keep track of the lowest numbered nodes in each block
	private static HashMap<Integer,Float> sampleNodes = new HashMap<Integer,Float>();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
		//Clean up all information at the beginning of each iteration
		nodeInblock.clear();
		boundaryCondition.clear();
		records.clear();
		updatedPageRank.clear();
		
		float oldPageRank = 0.0f;
		float residualError = 0.0f;
		String reducevalue = "";
		ArrayList<String> trackInblock = new ArrayList<String>();
		float trackBoundCondition = 0.0f;
		
		Iterator<Text> iter = values.iterator();
		while(iter.hasNext()){
			String[] valueArray = iter.next().toString().trim().split(" "); 
			String name = valueArray[0];
			//Extract the key parameters of the node and store them in the records
			if(name.equals("is_node")){
				oldPageRank = Float.parseFloat(valueArray[2]);
				updatedPageRank.put(valueArray[1], oldPageRank);
				webNode node = new webNode(valueArray[1],Float.parseFloat(valueArray[2]),0,"");
				if(valueArray.length == 4){
					int outdegree = valueArray[3].split(",").length;
					node.setoutDegree(outdegree);
					node.setOutgoingLinks(valueArray[3]);
				}
				records.put(node.getNodeID(),node);
			}
			//Recover the structure within the block
			if(name.equals("same_block")){
				trackInblock = nodeInblock.containsKey(valueArray[2])? nodeInblock.get(valueArray[2]):new ArrayList<String>();
				trackInblock.add(valueArray[1]);
				nodeInblock.put(valueArray[2],trackInblock);
			}
			//Track boundary condition
			if(name.equals("differ_block")){
				trackBoundCondition = boundaryCondition.containsKey(valueArray[2])?boundaryCondition.get(valueArray[2]):0.0f;
				trackBoundCondition += Float.parseFloat(valueArray[3]);
				boundaryCondition.put(valueArray[2],trackBoundCondition);
			}
		}
		
		int flag = 0;
		do {
			residualError = IterateBlockOnce();
			flag++;
		} while (residualError > randomBlockPartition_reduce.stoppingThreshold);
				
		context.getCounter(accumulator.counter.ITERATION).increment(flag);
		
		residualError = 0.0f;
		for(String k : records.keySet()){
			residualError += Math.abs(records.get(k).getPageRank()-updatedPageRank.get(k)) / updatedPageRank.get(k);
		}
		residualError /= records.size();
		
		//Accumulate error over the whole graph
		context.getCounter(accumulator.counter.RESIDUALERROR).increment((long)(residualError*100000000));
		
		//Send the node information back to the map
		for(String k : records.keySet()){
			webNode node = records.get(k);
			reducevalue = updatedPageRank.get(k)+" "+node.getoutDegree()+" "+node.getOutgoingLinks();
			context.write(new Text(k), new Text(reducevalue));
		}
		cleanup(context);
	}
	

	/**
	 * Compute the residual error over the whole block.
	 */
	public float IterateBlockOnce(){
		float res_error = 0.0f;
		for(String k : records.keySet()){
			float currPageRank = 0.0f;
			float oldPageRank = updatedPageRank.get(k);
			//Update PageRank using old PageRank information from nodes within the block
			if(nodeInblock.containsKey(k)){
				for(String l : nodeInblock.get(k)){
					currPageRank += (updatedPageRank.get(l) / records.get(l).getoutDegree());
				}
			}
			//Update PageRank using boundary conditions
			if(boundaryCondition.containsKey(k)){
				currPageRank += boundaryCondition.get(k);
			}
			
			currPageRank = randomBlockPartition_reduce.randomJump*currPageRank+((float)(1-randomBlockPartition_reduce.randomJump))/randomBlockPartition.numOfwebGraphNodes;
			updatedPageRank.put(k,currPageRank);
			sampleNodes.put(Integer.parseInt(k),currPageRank);
			res_error += Math.abs(oldPageRank - currPageRank) / currPageRank;
		}
		return res_error / records.size();
	}
	
	/**
	 * Get the lowest numbered node in each block
	 */	
	public static HashMap<Integer, Float> getSampleNodes(){
		return sampleNodes;
	}		
}	

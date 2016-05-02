package simplePageRank;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class simplePageRank_map extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		String[] parameter = getParameters(value);
		Text nodeID = new Text(parameter[0]);
		float PageRank = Float.parseFloat(parameter[1]);
		int outDegree = Integer.parseInt(parameter[2]);
		String outgoingLinks = parameter.length == 4? parameter[3]:"";
		//Pass along graph structure
		context.write(new Text(nodeID), new Text("is_node " + PageRank + " " + outgoingLinks));
		
		//Pass around PageRank contributions from current node to its out-neighbors
		if (outgoingLinks != "") {	
			float v = PageRank / outDegree;
			String[] outlinks = outgoingLinks.split(",");

			for (String S : outlinks) {
				context.write(new Text(S), new Text(""+v));
			}
		}	
	}
	public String[] getParameters(Text value){
		return value.toString().trim().split("\\s+");
	}
}
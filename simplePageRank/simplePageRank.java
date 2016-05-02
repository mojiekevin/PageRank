package simplePageRank;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class simplePageRank {
	
	//number of nodes in the graph
	public static final int numOfwebGraphNodes = 685230;
	//number of MapReduce passes
	public static final int numOfMapReducePass = 5;
	
	public static void main(String[] args) throws IOException{
		String inputFile = args[0];
		String outputPath = args[1];
		for(int i = 0; i < numOfMapReducePass; i++){
			Job job = simplePageRank.getMapReduceJob();
			if (i == 0){
				FileInputFormat.addInputPath(job, new Path(inputFile));
			} 
			else{
				FileInputFormat.addInputPath(job, new Path(outputPath+"/pass"+i));
			}
			FileOutputFormat.setOutputPath(job, new Path(outputPath+"/pass"+(i+1)));
			try{
				job.waitForCompletion(true);
			} catch(Exception e){
				System.err.println("Error in Job: " + e);
				return;
			}
			//Compute the average residual error in this iteration
			float avgResidualError = (((float)job.getCounters().findCounter(accumulator.counter.RESIDUALERRORS).getValue())/100000000)/numOfwebGraphNodes;
			System.out.println("average residual error in iteration " + i + ": " + avgResidualError);
			job.getCounters().findCounter(accumulator.counter.RESIDUALERRORS).setValue(0L);
		}
	}
	
	public static Job getMapReduceJob() throws IOException{
	
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(simplePageRank.class);
		job.setMapperClass(simplePageRank_map.class);
		job.setReducerClass(simplePageRank_reduce.class);		
		return job;	
	}			
	
}

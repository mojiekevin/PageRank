package randomBlockPartition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class randomBlockPartition {
	
	//number of nodes in the graph
	public static final int numOfwebGraphNodes = 685230;
	//number of blocks in the graph
	public static final int numOfBlocks = 68;	
	//threshold used to determine when to stop iteration
	public static final float threshold = 0.001f;
	
	public static void main(String[] args) throws Exception{
		String inputFile = args[0];
		String outputPath = args[1];
		float avgResidualError = 0.0f;
		float numOfIteration = 0.0f;
		int flag = 0;
		do{
			Job job = randomBlockPartition.getMapReduceJob();
			if (flag == 0){
				FileInputFormat.addInputPath(job, new Path(inputFile));
			} 
			else{
				FileInputFormat.addInputPath(job, new Path(outputPath+"/pass"+flag));
			}
			FileOutputFormat.setOutputPath(job, new Path(outputPath+"/pass"+(flag+1)));
			try{
				job.waitForCompletion(true);
			} catch(Exception e){
				System.err.println("Error in Job: " + e);
				return;
			}
			//Compute the residual error of the whole graph in this iteration
			avgResidualError = (((float)job.getCounters().findCounter(accumulator.counter.RESIDUALERROR).getValue())/100000000)/numOfBlocks;
			System.out.println("residual error in pass " + flag + ": " + avgResidualError);
			job.getCounters().findCounter(accumulator.counter.RESIDUALERROR).setValue(0L);
			//Compute the average number of iterations performed per block 
			numOfIteration = ((float)(job.getCounters().findCounter(accumulator.counter.ITERATION).getValue())/numOfBlocks);
			System.out.println("average number of iteration per block in pass " + flag + ": " + numOfIteration);
			flag++;
		}while(avgResidualError > threshold);
		//Entire computation has converged, report the sample of PageRank values.
		HashMap<Integer,Float> sample = randomBlockPartition_reduce.getSampleNodes();
		ArrayList<Integer> keys = new ArrayList<Integer>();
		for(Integer i : sample.keySet()){
			keys.add(i);
		}
		Collections.sort(keys);
		int blockID = 0;
		for(Integer j : keys){
			if(randomBlockPartition_map.checkInblocks(j)){
				System.out.println("block:"+blockID+" node:"+j+" PageRank:"+sample.get(j));
				System.out.println("block:"+blockID+" node:"+(j+1)+" PageRank:"+sample.get(j+1));
				blockID++;
			}
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
		job.setJarByClass(randomBlockPartition.class);
		job.setMapperClass(randomBlockPartition_map.class);
		job.setReducerClass(randomBlockPartition_reduce.class);		
		return job;	
	}
}

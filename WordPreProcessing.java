
import java.io.*;
import java.util.Hashtable;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

public class WordPreProcessing {
	
	private Hashtable<Integer, List<Integer>> graph = new Hashtable<>();
	private String fileName = "/Users/jd/Documents/courses_16sp/cs5300/proj2/edges.txt";
	/**
	 *  change to your ID
	 */
	private double fromNetID = 0.8542;
	public  void readFile() throws IOException  {

        BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(fileName));
		
        	
            String line = null;

            while ((line = br.readLine()) != null) {
              line = line.trim();
              
              String[] tokens = line.split("\\s+");
              
              int nodeID = Integer.parseInt(tokens[0]);
              int destination = Integer.parseInt(tokens[1]);
              double edgeWeight = Double.parseDouble(tokens[2]);
              if(graph.containsKey(nodeID)) {
            	  if(selectInputLine(edgeWeight)) {
            		  graph.get(nodeID).add(destination);  
            	  }
              } else {
            	  List<Integer> destList = new ArrayList<Integer>();
            	  if(selectInputLine(edgeWeight)) {
            		  destList.add(destination);
            	  } 
            	  graph.put(nodeID, destList);
              }
              if(!graph.containsKey(destination)){
            	  List<Integer> destList = new ArrayList<Integer>();
            	  graph.put(destination, destList);
              }
            }

        } catch (IOException e) {
             System.out.println(e);
        }finally {
            	br.close();
			
        }


        
		
	}
	
	public boolean selectInputLine(double x) {
		
		double rejectMin = 0.9 * this.fromNetID;
		double rejectLimit = rejectMin +  0.01;		
		return   ( ((x >= rejectMin) && (x < rejectLimit)) ? false : true );
	}
	
	
	public void writeFile() throws IOException {
		BufferedWriter output = null;
		File file = new File("/Users/jd/Documents/courses_16sp/cs5300/proj2/nodes.txt");
		 output = new BufferedWriter(new FileWriter(file));
        try {
           
            Set<Integer> keys = graph.keySet();
            List<Integer> nodeIDs = new ArrayList<Integer>(keys);
            
            Collections.sort(nodeIDs);
            float initRank = (float) 1.000000 /  (float)nodeIDs.size();
            for(Integer key : nodeIDs) {
            	List<Integer> adjNodes = graph.get(key);
            	String singleLine = "";
            	singleLine += key + " ";
            	//singleLine += String.format("%.15f", initRank);
            	singleLine += initRank;
            	singleLine += " ";
            	String value = "";
            	for(int adjnode : adjNodes) {
            		value += adjnode;
            		value += ",";
            	}
            	String value2 = value;
            	int size = 0;
            	if (value.trim().length() != 0) {
            		size = value.split(",").length;
            	} 
            	
            	singleLine += size + " " + value2;
            	singleLine = singleLine.substring(0, singleLine.length() - 1);
            	singleLine += "\n";
            	output.write(singleLine);
            	
            }
           
        } catch (IOException e) {
            System.out.println(e);
        } finally {
            output.close();
        }
	}
	
	public static void main(String[] args) throws IOException{
       WordPreProcessing process = new WordPreProcessing();
       process.readFile();
       process.writeFile();


    }
	
	
  

}

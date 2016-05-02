package blockPageRankPrint;
/**
 * 
 * @author kevin
 * 
 * This class contains key parameters of a node in a block as well as 
 * functions to get or set values of these parameters
 */
public class webNode {
	public String nodeID;
	public float PageRank;
	public int outDegree;
	public String outgoingLinks = "";
	
	/**
	 * Construct a node with given parameters
	 */
	public webNode(String id, float p, int d, String l){
		this.nodeID = id;
		this.PageRank = p;
		this.outDegree = d;
		this.outgoingLinks = l;
	}
	
	/**
	 * Get nodeID of the node.
	 */
	public String getNodeID(){
		return nodeID;
	}
	
	/**
	 * Get the value of PageRank.
	 */
	public float getPageRank(){
		return PageRank;
	}
	
	/**
	 * Get the value of out degrees.
	 * @return 
	 */
	public int getoutDegree(){
		return outDegree;
	}
	
	/**
	 * Get the outgoing links.
	 */
	public String getOutgoingLinks(){
		return outgoingLinks;
	}
	
	/**
	 * Set nodeID of the node.
	 */
	public void setNodeID(String id){
		nodeID = id;
	}
	
	/**
	 * Set the value of PageRank.
	 */
	public void SetPageRank(float p){
		PageRank = p;
	}
	
	/**
	 * Set the value of out degrees.
	 */
	public void setoutDegree(int d){
		outDegree = d;
	}
	
	/**
	 * Set the outgoing links.
	 */
	public void setOutgoingLinks(String s){
		outgoingLinks = s;
	}
}

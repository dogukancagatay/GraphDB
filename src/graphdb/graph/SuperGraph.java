package graphdb.graph;

import graphdb.util.Property;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

public class SuperGraph extends Graph {
	private static Logger log = Logger.getLogger(SuperGraph.class.getName());

	public SuperGraph(int gid, String graphName, long nodeIdCounter, long nodeCounter) {
		super(gid, graphName, nodeIdCounter, nodeCounter);
	}

	public SubGraph getSubgraphOfNode(Node node) {
		node.ifDummy();// if node is dummy bring load it's subgraph from db
		if(nodeIndex.containsKey(node.getId())){
			return subgraphMap.get(nodeIndex.get(node.getId()));
		}

		return null;

	}

	//called by db functions so no need to bring node from db
	public SubGraph getSubgraphOfNode(long nodeid){
		if(nodeIndex.containsKey(nodeid)){
			return subgraphMap.get(nodeIndex.get(nodeid));
		}

		return null;
	}

	public boolean hasNode(long nodeid){
		if(getSubgraphOfNode(nodeid) == null){
			return false;
		}
		return true;
	}

	public boolean hasNode(Node node){
		if(getSubgraphOfNode(node) == null){
			return false;
		}
		return true;
	}

	public SubGraph getSubgraph(int sgid) {
		if(subgraphMap.containsKey(sgid)){
			return subgraphMap.get(sgid);
		}

		return null;
	}

	public Node addNode(long nodeId, int sgid){
		SubGraph sg = getSubgraph(sgid);
		if(sg == null)
			throw new NoSuchElementException("Subgraph could not be found: " + sgid);

		//if node doesn't exist in the subgraph
		//		if(sg.getNode(nodeId) == null){
		if(!sg.nodeMap.containsKey(nodeId)){
			nodeIndex.put(nodeId, sgid);
			return sg.addNode(new Node(this,nodeId));
		}
		else {
			return sg.getNode(nodeId);
		}
	}

	public Node addDummyNode(long nodeId){
		//adding dummy node means that it just a placeholder,
		//not associated with any subgraph.
		Node n = new Node(this, nodeId);
		n.setDummy(true);
		return n;
	}

	public Node addNode(long nodeId){
		SubGraph sg = GraphManager.getInstance().getSubgraphSelectPolicy().select(null, this);

		//if node doesn't exist in the subgraph
		if(sg.getNode(nodeId) == null){
			nodeIndex.put(nodeId, sg.getId());
			return sg.addNode(new Node(this,nodeId));
		}
		else {
			return sg.getNode(nodeId);
		}
	}

	public Edge batchAddEdge(long srcid, long destid){
		//bring both nodes from database
		Node src = getNode(srcid);
		Node dest = getNode(destid);

		//add edge to the source subgraph
		Edge newEdge = new Edge(src, dest);

		src.ifDummy();
		SubGraph srcsg =  subgraphMap.get(nodeIndex.get(src.getId()));
		srcsg.addOutgoingEdge(newEdge);

		//if dest somehow is written to db then bring it back
		dest.ifDummy();

		SubGraph destsg = subgraphMap.get(nodeIndex.get(dest.getId()));
		destsg.addIncomingEdge(newEdge);

		return newEdge;
	}

	public void newAddEdge(long srcid, long destid, int sgid) {
		//		SubGraph srcsg,destsg;
		Node src = null;
		Node dest = null;

		//if the nodes are not yet read from database
		if(!nodeIndex.containsKey(srcid)){
			src = addNode(srcid, sgid);
		}
		else {
			src = getNode(srcid);
		}

		if(!nodeIndex.containsKey(destid)){
			dest = addNode(destid, sgid);
		}
		else {
			dest = getNode(destid);
		}

		Edge e = null;
		for(Edge d : src.getEdges(EdgeDirection.OUT)){
			if(d.getDestinationId() == destid){
				e = d;
				break;
			}
		}

		if(e == null){ // couldn't find in the out edges of src
			// Graph.addEdge handles everything (new edge)
			e = addEdge(src, dest);
		}
	}

	public void addEdge(long srcid, long destid, int sgid) {
		Node src = null;
		Node dest = null;

		//if the nodes are not yet read from database
		if(!nodeIndex.containsKey(srcid)){
			src = addNode(srcid, sgid);
		}
		else {
			src = getNode(srcid);
		}

		if(!nodeIndex.containsKey(destid)){
			dest = addNode(destid, sgid);
		}
		else {
			dest = getNode(destid);
		}

		Edge e = null;
		for(Edge d : src.getEdges(EdgeDirection.OUT)){
			if(d.getDestination().getId() == destid){
				e = d;
				break;
			}
		}

		if(e == null){ // couldn't find in the out edges of src
			for(Edge s : dest.getEdges(EdgeDirection.IN)){
				if(s.getSource().getId() == srcid){
					e = s;
					break;
				}
			}
			if(e == null){ // couldn't find in the in edges of dest
				// Graph.addEdge handles everything (new edge)
				e = addEdge(src, dest);
			}
		}
	}
	public Node addNodeFromDb(long nodeId, int sgid){
		SubGraph sg = getSubgraph(sgid);
		if(sg == null)
			throw new NoSuchElementException("Subgraph could not be found: " + sgid);

		//if node doesn't exist in the subgraph
		//		if(sg.getNode(nodeId) == null){
		if(!sg.nodeMap.containsKey(nodeId)){
			nodeIndex.put(nodeId, sgid);
			return sg.addNodeFromDb(new Node(this,nodeId));
		}
		else {
			return sg.getNode(nodeId);
		}
	}

	public Edge addOutgoingEdgeFromDb(long srcid, long destid, int sgid) {
		Node src = null;
		Node dest = null;

		/*get the source node (which is already added)
		and don't bring destination if it is not already in the buffer
		add a dummy one */

		src = getNode(srcid);

		if(!nodeIndex.containsKey(destid)){
			dest = addDummyNode(destid);
		}
		//		else {
		//			dest = getNode(destid);
		//		}
		//3 lines above is commented for the 2 lines below
		dest = new Node(this,destid);
		dest.setDummy(true);

		Edge newEdge = new Edge(src, dest);
		subgraphMap.get(sgid).addOutgoingEdgeFromDb(newEdge);

		return newEdge;
	}

	public Edge addIncomingEdgeFromDb(long srcid, long destid, int sgid) {
		Node src = null;
		Node dest = null;

		/* get the dest node (which is already added)
		and don't bring source if it is not already in the buffer
		add a dummy one */

		dest = getNode(destid);

		if(!nodeIndex.containsKey(srcid)){
			src = addDummyNode(srcid);
		}
		//		else {
		//			src = getNode(srcid);
		//		}
		//3 lines above is commented for the 2 lines below
		src = new Node(this,srcid);
		src.setDummy(true);

		Edge newEdge = new Edge(src, dest);
		subgraphMap.get(sgid).addIncomingEdgeFromDb(newEdge);

		return newEdge;
	}

	public SubGraph createSubgraph(long pageid){
		//TODO: include page creation in this subgraph creation
		//		Page newpage = PageManager.getInstance().createNewPage(superg.getId(), -1);
		//		SubGraph newsg = superg.createSubgraph(newpage.getId());
		//		newpage.setSubgraph(newsg.getId());
		//		newsg.setDirty(true);

		SubGraph sg = new SubGraph(subgraphCounter++, pageid, this);
		subgraphMap.put(sg.getId(), sg);

		return sg;
	}

	public void moveNode(Node node,SubGraph oldsg, SubGraph newsg){
		long nodeid = node.getId();

		if(node != null && oldsg != null && newsg != null){

			List<Edge> outEdgeList = oldsg.outgoingEdgeMap.get(nodeid);
			List<Edge> inEdgeList = oldsg.incomingEdgeMap.get(nodeid);

			//add to new subgraph
			newsg.nodeMap.put(nodeid, node);

			//update byte count of old subgraph
			oldsg.decByteCount(oldsg.getSize(node));

			//change node -> subgraph index map
			nodeIndex.put(nodeid, newsg.getId());

			if(inEdgeList != null){
				newsg.incomingEdgeMap.put(nodeid, inEdgeList);
				oldsg.incomingEdgeMap.remove(nodeid);
			}

			if(outEdgeList != null){
				newsg.outgoingEdgeMap.put(nodeid, outEdgeList);
				oldsg.outgoingEdgeMap.remove(nodeid);
			}

			//update byte count of old subgraph
			newsg.incByteCount(newsg.getSize(node));

			//if the moved node is big enough to hold whole subgraph then don't add any more nodes to it
			if(newsg.getTotalSize() >= GraphManager.getInstance().getBufferConfiguration().getMaxBlockSize() ){
				newsg.setPartitioned(true);
			}

			//            log.trace("Node" + node.getId() + " moved from " + oldsg + " to " + newsg);
		}

	}

	public void setNodeIdCounter(long cnt){
		this.nodeIdCounter = cnt;
	}

	public void setNodeCounter(long cnt){
		this.nodeCounter = cnt;
	}

	public HashSet<Edge> getEdgeSet() {
		return edgeSet;
	}

	/* Property Handling */
	public void setPropertyObject(Node n, Property p){
		nodeProps.put(n.getId(), p);
		getSubgraphOfNode(n.getId()).addPropertyCost(p.getObjectSize());
	}

	public void setPropertyObject(Edge e, Property p, long nid){
		if(!edgeProps.containsKey(e)){
			edgeProps.put(e, p);
		}

		if(nodeIndex.containsKey(nid)){
			getSubgraphOfNode(nid).addPropertyCost(p.getObjectSize());
		}
	}

	public boolean nodeHasProperty(long nodeid){
		return nodeProps.containsKey(nodeid);
	}

	public boolean edgeHasProperty(Node src, Node dest){
		return edgeProps.containsKey(new Edge(src, dest));
	}

	public Property getPropertyObject(Node node){
		return nodeProps.get(node.getId());
	}

	public Property getPropertyObject(Edge edge){
		return edgeProps.get(edge);
	}

	public void removePropertyObject(Edge e) {
		getSubgraphOfNode(e.getSource()).removeProperty(e);

		if(e.isCross()){
			getSubgraphOfNode(e.getDestination()).removeProperty(e);
		}

		edgeProps.remove(e);
	}

	public void removePropertyObject(Node n) {
		getSubgraphOfNode(n).removeProperty(n);
		nodeProps.remove(n);
	}

	public HashMap<Edge, Property> getEdgeProps() {
		return edgeProps;
	}

	public Graph getGraph() {
		return (Graph) this;
	}

	public SuperGraph getSuperGraph() {
		return this;
	}

	public long getNodeIdCount() {
		return nodeIdCounter;
	}

	public void remove(SubGraph sg) {
		subgraphMap.remove(sg.getId());
	}

	public void swapDummyNode(Node node) {
		SubGraph sg = getSubgraphOfNode(node.getId());
		if(sg != null){
			sg.swapDummyNode(node);
		}
	}

	public boolean isCross(long srcid, long destid){
		SubGraph srcsg = getSubgraphOfNode(srcid);
		SubGraph destsg = getSubgraphOfNode(srcid);

		//if both are in memory
		if(srcsg != null && destsg != null){
			//if both are the same subgraph
			if(srcsg.equals(destsg)){
				return false;
			}
		}

		//this is not real truth about this edge being cross
		//it does not look for nodes' block metadata on disk
		//this function assumes at least one of the nodes is
		//in memory
		return true;
	}
	public boolean onDisk(long nodeid){
		//this function doesn't check the existence of the node in the graph
		return !nodeIndex.containsKey(nodeid);
	}
}

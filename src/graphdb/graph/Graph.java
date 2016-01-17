package graphdb.graph;

import graphdb.util.CollectionsIterator;
import graphdb.util.Property;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

public class Graph {
	protected int gid;
	protected long nodeIdCounter;
	protected long nodeCounter;
	//	protected long edgeIdCounter;
	protected int subgraphCounter;
	protected String graphName;

	private static Logger log = Logger.getLogger(Graph.class.getName());

	protected HashMap<Integer, SubGraph> subgraphMap; // id -> subgraph
	public HashMap<Long, Integer> nodeIndex; // node id -> subgraph
	public HashSet<Edge> edgeSet; // edge

	protected HashMap<Long, Property> nodeProps;
	protected HashMap<Edge, Property> edgeProps;

	protected Graph(int gid, String graphName, long nodeIdCounter, long nodeCounter) {
		this.gid = gid;
		this.graphName = graphName;

		subgraphMap = new HashMap<Integer, SubGraph>();
		edgeSet =  new HashSet<Edge>();
		nodeIndex = new HashMap<Long, Integer>();

		nodeProps = new HashMap<Long, Property>();
		edgeProps = new HashMap<Edge, Property>();

		this.nodeIdCounter = nodeIdCounter;
		this.nodeCounter = nodeCounter;

		subgraphCounter = 0;
	}

	public int getId(){
		return gid;
	}

	public String getName(){
		return graphName;
	}

	public void setName(String graphName){
		try {
			GraphManager.getInstance().getGraph(graphName);
			throw new RuntimeException("Another graph with the same name (" + graphName + ") exists. Graph name identifiers must be unique.");
		} catch (NoSuchElementException e) {
			this.graphName = graphName;
		}
	}

	public List<Long> getNodeList(){
		return GraphManager.getInstance().getNodeList(gid);
	}

	public Node getNode(long nodeid) {
		if(nodeIndex.containsKey(nodeid) && subgraphMap.get(nodeIndex.get(nodeid)) != null){
			return subgraphMap.get(nodeIndex.get(nodeid)).getNode(nodeid);
		}
		else {
			//request the node from database
			GraphManager.getInstance().bringNode(nodeid, gid);

			if(nodeIndex.containsKey(nodeid)){
				return subgraphMap.get(nodeIndex.get(nodeid)).getNode(nodeid);
			}
			else {
				throw new RuntimeException("Error bringing node from db : "+ nodeid);
			}
		}

		//		return null;
	}

	public Iterable<Node> getNodes(){
		final SuperGraph superg = (SuperGraph) this;
		BufferManager.getInstance().flushBuffer();

		return new Iterable<Node>(){

			@Override
			public Iterator<Node> iterator() {
				CollectionsIterator<Node> colit = new CollectionsIterator<Node>();

				final Iterator<Long> nodeIdIterator = GraphManager.getInstance().getNodes(gid);

				colit.addCollection(new Iterator<Node>(){

					@Override
					public boolean hasNext() {
						return nodeIdIterator.hasNext();
					}

					@Override
					public Node next() {
						return new Node(superg, nodeIdIterator.next().longValue(), true);
					}

					@Override
					public void remove() {
						nodeIdIterator.remove();
					}});

				return colit;
			}
		};
	}
	public Iterable<Edge> getEdges(Node node){
		log.trace("Edges of node " + node + " is requested.");
		return subgraphMap.get(nodeIndex.get(node.getId())).getEdges(node);
	}

	public Iterable<Edge> getEdges(Node node, EdgeDirection direction){
		log.trace("Directional edges of node" + node + " is requested: " + direction);

		SubGraph sg = ((SuperGraph) this).getSubgraphOfNode(node);
		if(sg == null){
			System.out.println("subgraph " + nodeIndex.get(node.getId()) +" for node "+ node.getId() + "is full");
			throw new NullPointerException();
		}
		return sg.getEdges(node,direction);
	}

	public int getNumEdges(Node node, EdgeDirection direction){
		return ((SuperGraph)this).getSubgraphOfNode(node).getNumEdges(node,direction);
	}

	public Node addNode(){
		SubGraph sg = GraphManager.getInstance().getSubgraphSelectPolicy().select(null, (SuperGraph) this);

		//for counting number of nodes;
		nodeCounter++;
		long nodeId = nodeIdCounter++;

		//relate node and selected subgraph
		nodeIndex.put(nodeId, sg.getId());

		//add node to subgraph
		Node n = sg.addNode(new Node((SuperGraph) this, nodeId));

		log.trace("Node added :" + n);

		return n;
	}

	public Edge addEdge(Node src, Node dest){
		if(src != null && dest != null){
			log.trace("An edge requested to be added: "+ src + "(isDummy:" + src.isDummy() + ")" + " -> " + dest + "(isDummy:" + dest.isDummy() + ")");

			SubGraph srcsg = ((SuperGraph)this).getSubgraphOfNode(src);

			//if the edge already exists dont add it
			Edge e = srcsg.findEdge(src.getId(),dest.getId());
			if(e != null){
				return e;
			}

			//add edge to the source subgraph
			Edge newEdge = new Edge(src, dest);
			srcsg.addOutgoingEdge(newEdge);

			SubGraph destsg = ((SuperGraph)this).getSubgraphOfNode(dest);
			destsg.addIncomingEdge(newEdge);

			GraphManager.getInstance().getSubgraphSplitPolicy().addEdgeAction(newEdge);

			log.trace("An edge added : "+ newEdge);
			return newEdge;
		}

		return null;
	}

	public Node setProperty(Node node, String key, Object value){
		int oldPropSize = 0;
		int newPropSize = 0;

		//if node already have a property object
		if(nodeProps.containsKey(node.getId())){
			Property prop = nodeProps.get(node.getId());

			//if this is an update and new value and old value differs
			if(prop.getProperty(key) != null && !prop.getProperty(key).equals(value)){
				//get old property size
				oldPropSize = prop.getObjectSize();

				//add the key and value to existing property
				prop.setProperty(key, value);

				//get new property size
				newPropSize = prop.getObjectSize();
			}
		}
		else {
			Property prop = new Property();
			prop.setProperty(key, value);

			//set new property object
			nodeProps.put(node.getId(), prop);

			//get new property size
			newPropSize = prop.getObjectSize();
		}

		((SuperGraph)this).getSubgraphOfNode(node).incByteCount(newPropSize - oldPropSize);

		return node;
	}

	public Edge setProperty(Edge edge, String key, Object value){
		int oldPropSize = 0;
		int newPropSize = 0;

		if(edgeProps.containsKey(edge)) {
			Property prop = edgeProps.get(edge);

			//if this is an update and new value and old value differs
			if(prop.getProperty(key) != null && !edgeProps.get(edge).getProperty(key).equals(value)){

				//get old property size
				oldPropSize = prop.getObjectSize();

				//add the key and value to existing property
				prop.setProperty(key, value);

				//get new property size
				newPropSize = prop.getObjectSize();
			}
		}
		else {
			//create a new property object
			Property prop = new Property();
			prop.setProperty(key, value);

			edgeProps.put(edge, prop);

			newPropSize = prop.getObjectSize();
		}

		// both src and dest is requested if they are both in the buffer
		// nothing happens but if one of them is not in the buffer, it is brought
		// and it is updated when it is being read.

		SubGraph srcsg = ((SuperGraph)this).getSubgraphOfNode(edge.getSource());

		//if src and destination subgraphs are identical
		if(srcsg.nodeMap.containsKey(edge.getDestinationId())){
			srcsg.incByteCount(2 * (newPropSize - oldPropSize));
		}
		else {
			srcsg.incByteCount(newPropSize - oldPropSize);

			SubGraph destsg = ((SuperGraph)this).getSubgraphOfNode(edge.getDestination());
			destsg.incByteCount(newPropSize - oldPropSize);
		}

		return edge;
	}

	public Object getProperty(Node node, String key) {
		Property props = nodeProps.get(node.getId());
		if(props == null){
			return null;
		}

		Object prop = props.getProperty(key);
		if(prop == null){
			return null;
		}

		return prop;
	}

	public Object getProperty(Edge edge, String key) {
		Property props = edgeProps.get(edge);
		if(props == null){
			return null;
		}

		Object prop = props.getProperty(key);
		if(prop == null){
			return null;
		}

		return prop;
	}

	public boolean hasProperty(Node node){
		return nodeProps.containsKey(node.getId());
	}

	public boolean hasProperty(Edge edge){
		return edgeProps.containsKey(edge);
	}

	public boolean removeProperty(Edge edge, String key){
		throw new NotImplementedException();
	}

	public boolean removeProperty(Node node, String key){
		throw new NotImplementedException();
	}

	public void remove(Node src, Node dest){
		SuperGraph superg = (SuperGraph) this;
		boolean state = true;

		//delete edge from src's subgraph
		SubGraph srcsg = superg.getSubgraphOfNode(src);
		srcsg.removeEdge(src, dest);

		SubGraph destsg = superg.getSubgraphOfNode(src);

		//delete edge from dest's subgraph if on different subgraph
		if(!srcsg.equals(destsg))
			state = destsg.removeEdge(src, dest);

		if(!state){
			Edge temp = new Edge(src,dest);
			edgeSet.remove(temp);
		}
	}

	public void remove(Edge edge){
		SuperGraph superg = (SuperGraph) this;

		//delete edge from src's subgraph
		SubGraph srcsg = superg.getSubgraphOfNode(edge.getSource());
		srcsg.removeEdge(edge.getSource(), edge.getDestination());

		SubGraph destsg = superg.getSubgraphOfNode(edge.getDestination());

		//delete edge from dest's subgraph if on different subgraph
		if(!srcsg.equals(destsg))
			destsg.removeEdge(edge.getSource(), edge.getDestination());

		edgeSet.remove(edge);
	}

	public void remove(Node node) {
		//get node's subgraph
		SuperGraph superg = (SuperGraph) this;

		SubGraph sg = superg.getSubgraphOfNode(node);
		sg.removeNode(node);

		nodeIndex.remove(node.getId());
		nodeProps.remove(node.getId());
	}

	public HashMap<Integer,SubGraph> getSubgraphMap(){
		return subgraphMap;
	}

	public long getNumNodes() {
		return nodeCounter;
	}

	public void delete(){
		throw new NotImplementedException();
	}

	public void printSubgraphs(String message){
		System.out.println(message);
		System.out.println("=== Subgraph Summary Starting===");
		for (Iterator<SubGraph> it = subgraphMap.values().iterator(); it.hasNext();) {
			SubGraph sg = it.next();

			System.out.println("Nodes in Subgraph " + sg.getId() + " (size=" + sg.getTotalSize() + ", size2=" + sg.getTotalSize2() + "Page"+ sg.pageid + ")");
			for (Iterator<Node> it2 = sg.nodeMap.values().iterator(); it2.hasNext();) {
				Node n = it2.next();

				System.out.println("\tNode " + n.getId() + ": ");

				System.out.print("\tIncoming Edges : ");
				if(sg.incomingEdgeMap.containsKey(n.getId())){
					System.out.println(sg.incomingEdgeMap.get(n.getId()));
				}
				else {
					System.out.println("None");
				}
				System.out.print("\tOutgoing Edges : ");
				if(sg.outgoingEdgeMap.containsKey(n.getId())){
					System.out.println(sg.outgoingEdgeMap.get(n.getId()));
				}
				else {
					System.out.println("None");
				}
			}
		}
		System.out.println("=== Subgraph Summary Finished ===");
		System.out.println();
	}
}

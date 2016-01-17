package graphdb.graph;

import graphdb.util.CollectionsIterator;
import graphdb.util.DoublyLinkedList;
import graphdb.util.Property;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

public class SubGraph{

	protected int sgid;
	protected long pageid;
	protected SuperGraph g;
	private boolean dirty;
	private boolean partitioned;
	private int bytecount;

	//byte costs
	public static final int blockInitCost = 4; //end of block descriptor
	public static final int nodeCost = 4 + 8; // node descriptor + node id
	public static final int edgeInitCost = 4 + 8; // edge descriptor + number of edges
	public static final int edgeCost = 8 + 4; // incident node id + property object length
	public static final int nodePropertyInitCost = 4 + 4; // property descriptor + property object length

	private static Logger log = Logger.getLogger(SubGraph.class.getName());

	public HashMap<Long, Node> nodeMap; // node id --> node
	public HashMap<Long, List<Edge>> outgoingEdgeMap; // edge id --> edge list
	public HashMap<Long, List<Edge>> incomingEdgeMap; // edge id --> edge list

	private final int MAX_BLOCK_SIZE = GraphManager.getInstance().getBufferConfiguration().getMaxBlockSize();

	protected SubGraph(int sgid, long pageid, SuperGraph g){
		this.sgid = sgid;
		this.pageid = pageid;
		this.g = g;
		this.dirty = false;
		this.partitioned = false;
		this.bytecount = blockInitCost;

		nodeMap = new HashMap<Long,Node>();

		outgoingEdgeMap = new HashMap<Long, List<Edge>>();
		incomingEdgeMap = new HashMap<Long, List<Edge>>();
	}

	public int getId() {
		return sgid;
	}

	public SuperGraph getGraph() {
		return g;
	}

	public Node getNode(long nodeId) {
		//issue access to the page in the buffer
		BufferManager.getInstance().issueAccessToBuffer(pageid);

		if(nodeMap.containsKey(nodeId)){
			return nodeMap.get(nodeId);
		}
		return null;
	}

	public Iterable<Node> getNodes(){
		//issue access to the page in the buffer
		BufferManager.getInstance().issueAccessToBuffer(pageid);

		return new Iterable<Node>(){

			@Override
			public Iterator<Node> iterator() {
				return nodeMap.values().iterator();
			}
		};
	}

	public Iterable<Edge> getEdges(final Node node){
		return getEdges(node, EdgeDirection.BOTH);
	}

	public Iterable<Edge> getEdges(final Node node, EdgeDirection direction){
		//issue access to the page in the buffer
		BufferManager.getInstance().issueAccessToBuffer(pageid);

		if(direction.equals(EdgeDirection.IN)){
			return new Iterable<Edge>(){

				@Override
				public Iterator<Edge> iterator() {
					if(node != null && incomingEdgeMap.get(node.getId()) != null){
						return incomingEdgeMap.get(node.getId()).iterator();
					}
					//if the node doesn't have an edge return empty list
					return new LinkedList<Edge>().iterator();
				}

			};
		}
		else if(direction.equals(EdgeDirection.OUT)){
			return new Iterable<Edge>(){

				@Override
				public Iterator<Edge> iterator() {
					if(node != null && outgoingEdgeMap.get(node.getId()) != null){
						return outgoingEdgeMap.get(node.getId()).iterator();
					}
					//if the node doesn't have an edge return empty list
					return new LinkedList<Edge>().iterator();
				}

			};
		}
		else if(direction.equals(EdgeDirection.BOTH)){
			return new Iterable<Edge>(){

				@Override
				public Iterator<Edge> iterator() {
					CollectionsIterator<Edge> colit = new CollectionsIterator<Edge>();

					colit.addCollection(getEdges(node, EdgeDirection.IN));
					colit.addCollection(getEdges(node, EdgeDirection.OUT));

					return colit;
				}
			};
		}
		else{
			return null;
		}
	}

	public int getNumEdges(final Node node, EdgeDirection direction){
		int numEdges = 0;

		log.trace("Number of edges for node" + node + " is requested.");
		if(direction.equals(EdgeDirection.IN) || direction.equals(EdgeDirection.BOTH)){
			if(incomingEdgeMap.containsKey(node.getId())){
				numEdges += (long)incomingEdgeMap.get(node.getId()).size();

				//				log.trace("Number of incoming edges for node" + node + ": " + incomingEdgeMap.get(node.getId()).size());
			}
		}

		if(direction.equals(EdgeDirection.OUT) || direction.equals(EdgeDirection.BOTH)){
			if(outgoingEdgeMap.containsKey(node.getId())){
				numEdges += (long)outgoingEdgeMap.get(node.getId()).size();

				//				log.trace("Number of outgoing edges for node" + node + ": " + outgoingEdgeMap.get(node.getId()).size());
			}
		}

		return numEdges;
	}

	public Node addNode(Node node){
		nodeMap.put(node.getId(), node);
		incByteCount(nodeCost);

		return node;
	}

	public void addOutgoingEdge(Edge edge){
		long srcId = edge.getSourceId();
		int edgesize = 0;

		if(!g.edgeSet.contains(edge)){
			g.edgeSet.add(edge);
		}

		// if the node is in outgoingEdgeMap
		if(outgoingEdgeMap.containsKey(srcId) && !outgoingEdgeMap.get(srcId).isEmpty()){

			// directly add to the list
			outgoingEdgeMap.get(srcId).add(edge);

			edgesize += edgeCost;
		}
		else {

			// create a new edge list for the node
			List<Edge> newEdgeList = new LinkedList<Edge>();
			newEdgeList.add(edge);

			// and add it to the outgoingEdgeMap
			outgoingEdgeMap.put(srcId,newEdgeList);

			//if it is the first edge of the node
			//add the initialization cost
			edgesize += edgeInitCost;
			edgesize += edgeCost;
		}

		incByteCount(edgesize);
	}

	public void addIncomingEdge(Edge edge){
		long destId = edge.getDestinationId();
		int edgesize = 0;

		if(!g.edgeSet.contains(edge)){
			g.edgeSet.add(edge);
		}

		// if the node is already in incomingEdgeMap
		if(incomingEdgeMap.containsKey(destId) && !incomingEdgeMap.get(destId).isEmpty()){

			// directly add to the list of edges
			incomingEdgeMap.get(destId).add(edge);

			edgesize += edgeCost;
		}
		else {
			// create a new edge list for the node
			List<Edge> newEdgeList = new LinkedList<Edge>();
			newEdgeList.add(edge);

			// and add it to the incomingEdgeMap
			incomingEdgeMap.put(destId, newEdgeList);

			//if it is the first edge of the node
			//add the initialization cost
			edgesize += edgeInitCost;
			edgesize += edgeCost;
		}

		incByteCount(edgesize);
	}

	public int getTotalSize(){
		return bytecount;
	}

	public int getTotalSize2(){
		int count = blockInitCost;
		for (Iterator<Node> it = nodeMap.values().iterator(); it.hasNext();) {
			Node n = it.next();
			count += getSize(n);
		}
		return count;
	}

	public int getSize(Node n) {
		int bytesize = nodeCost;

		if(n == null){
			return 0;
		}

		if(incomingEdgeMap.containsKey(n.getId())){

			bytesize += edgeInitCost;
			bytesize += incomingEdgeMap.get(n.getId()).size() * edgeCost;

			for (Iterator<Edge> it = incomingEdgeMap.get(n.getId()).iterator(); it.hasNext();) {
				Edge e = it.next();

				if(e.hasProperty())
					bytesize += g.getPropertyObject(e).getObjectSize();

			}
		}

		if(outgoingEdgeMap.containsKey(n.getId())){

			bytesize += edgeInitCost;
			bytesize += outgoingEdgeMap.get(n.getId()).size() * edgeCost;

			for (Iterator<Edge> it = outgoingEdgeMap.get(n.getId()).iterator(); it.hasNext();) {
				Edge e = it.next();

				if(e.hasProperty())
					bytesize += g.getPropertyObject(e).getObjectSize();
			}
		}

		if(n.hasProperty()){
			bytesize += g.getPropertyObject(n).getObjectSize();
		}

		return bytesize;
	}

	public List<Long> writeSubgraphToPage() {

		Page page = PageManager.getInstance().getPage(pageid);
		List<Long> nodeList = new LinkedList<Long>();

		//get the leading page which we are going to write to
		Page currentPage = PageManager.getInstance().getPage(pageid).getAdjacentPagesList().getFirstElement();

		//get the next page if the pages are partitioned.
		DoublyLinkedList.Node<Page> nextPageNode = currentPage.getAdjacentPagesList().getFirstNode().next;

		ByteBuffer bb = null;

		long start = System.currentTimeMillis();
		int edgecnt = 0;

		//		    ObjectOutputStream oos = new ObjectOutputStream(baos);
		bb = ByteBuffer.allocate(getTotalSize());

		for (Iterator<Long> it = nodeMap.keySet().iterator(); it.hasNext();) {
			long nodeid = it.next();

			//                oos.writeInt(1);// node start descriptor
			//                oos.writeLong(nodeid); // node id
			bb.putInt(1);// node start descriptor
			bb.putLong(nodeid); // node id

			nodeList.add(nodeid);

			//set node being written to the db as dummy for further usage
			nodeMap.get(nodeid).setDummy(true);

			//check if there are incoming edges
			if(incomingEdgeMap.containsKey(nodeid) && incomingEdgeMap.get(nodeid).size() > 0){
				//                    oos.writeInt(2); // incoming edge start descriptor
				bb.putInt(2); // incoming edge start descriptor

				//size of incoming edges
				//                    oos.writeLong((long) incomingEdgeMap.get(nodeid).size());
				bb.putLong((long) incomingEdgeMap.get(nodeid).size());

				edgecnt += incomingEdgeMap.get(nodeid).size();

				for (Iterator<Edge> it2 = incomingEdgeMap.get(nodeid).iterator(); it2.hasNext();) {
					Edge e = it2.next();

					//                        oos.writeLong(e.getSourceId()); //write source id to the stream
					bb.putLong(e.getSourceId()); //write source id to the stream

					//if incident node of incoming edge is in the same block write null
					//because if they are in the same block they will always be loaded at the same time
					if(!g.hasProperty(e) || nodeMap.containsKey(e.getSourceId())){
						//                            oos.writeObject(null);
						bb.putInt(-1);
					}
					else {
						byte[] propData = Property.writePropertyObject(g.getPropertyObject(e));
						//							    oos.writeObject(g.getPropertyObject(e));
						bb.putInt(propData.length); //put property length
						log.debug("Property data length = " + propData.length);
						//						System.out.println("bb capacity : " + bb.capacity());
						//						System.out.println("bb position : " + bb.position());
						//						System.out.println("bb remaining : " + bb.remaining());
						//						System.out.println("node of sg : " + nodeList.size() + "/" + nodeMap.size());
						bb.put(propData); //put property

						//if adjacent node is not in the buffer we may remove the property object
						if(!g.nodeIndex.containsKey(e.getSourceId())){
							g.edgeProps.remove(e);
						}
					}
				}
				//delete the incoming edge list of the node
				//                    incomingEdgeMap.get(nodeid).clear();
				incomingEdgeMap.remove(nodeid);
			}

			//check if there are outgoing edges
			if(outgoingEdgeMap.containsKey(nodeid) && outgoingEdgeMap.get(nodeid).size() > 0){
				//                    oos.writeInt(3); // incoming edge start descriptor
				bb.putInt(3); // incoming edge start descriptor

				//size of outgoing edges
				//                    oos.writeLong((long) outgoingEdgeMap.get(nodeid).size());
				bb.putLong((long) outgoingEdgeMap.get(nodeid).size());

				edgecnt += outgoingEdgeMap.get(nodeid).size();

				for (Iterator<Edge> it2 = outgoingEdgeMap.get(nodeid).iterator(); it2.hasNext();) {
					Edge e = it2.next();

					//                        oos.writeLong(e.getDestinationId()); //write destination id to the stream
					bb.putLong(e.getDestinationId()); //write destination id to the stream

					//in any case outgoing edge property would be written to the block.
					//                        oos.writeObject(g.getPropertyObject(e));
					if(!g.hasProperty(e)){
						bb.putInt(-1);
					}
					else{
						byte[] propData = Property.writePropertyObject(g.getPropertyObject(e));
						//							    oos.writeObject(g.getPropertyObject(e));
						bb.putInt(propData.length); //put property length
						bb.put(propData); //put property

						//if adjacent node is in the same block or not in the buffer remove the property
						if(nodeMap.containsKey(e.getDestinationId())){
							g.edgeProps.remove(e);
						}
						else {
							// if adjacent node is not in the buffer
							if(!g.nodeIndex.containsKey(e.getDestinationId())){
								g.edgeProps.remove(e);
							}
						}
					}
				}

				//delete the outgoing edge list of the node
				//                    outgoingEdgeMap.get(nodeid).clear();
				outgoingEdgeMap.remove(nodeid);
			}

			//if the node has property to be written
			if(g.nodeHasProperty(nodeid)){
				bb.putInt(4); // node property descriptor
				Property nodeprop = g.getPropertyObject(nodeMap.get(nodeid));

				byte[] propData = Property.writePropertyObject(nodeprop);
				bb.putInt(propData.length); //put property length
				bb.put(propData); //put property

				//                    oos.writeObject(nodeprop);

			}

		}// end of node iterating for

		start = System.currentTimeMillis() - start;
		if(start > 60){
			log.info("Invalidating subgraph : " + start);
			log.info("Subgraph node num : " + nodeMap.size());
			log.info("Edge num : " + edgecnt);
		}

		//            oos.writeInt(0); // end of page
		bb.putInt(0);

		if(partitioned){
			//get the byte array list from seq stream
			//			    List<byte[]> balist = ((SequenceByteArrayOutputStream) baos).toByteArrayList();
			List<byte[]> balist = partitionByteArray(bb.array());

			//TODO: if it was partitioned and now it is not set partitioned false (shrink protocol)

			//if it was not partitioned before but now it has to be partitioned (!partitioned)
			setPartitioned(true);

			for (Iterator<byte[]> it = balist.iterator(); it.hasNext();) {
				byte[] bs = it.next();

				if(bs.length > 0){

					//set current block's data
					currentPage.setData(bs);
					currentPage.setPartitioned(true);

					//if there are page(s) to write
					if(it.hasNext()){

						//if there are no more adjacent pages to write
						//and there is more data to write, create new one
						if(nextPageNode == null){
							//create a new page in the adjacent page list
							//1 doesn't create new adjpage list
							Page newpage = PageManager.getInstance().createNewPartitionedPage(g.getId(), sgid, currentPage.getAdjacentPagesList());

							currentPage = newpage;
							nextPageNode = null; //next time a new page will be created as well
						}
						else{
							currentPage = nextPageNode.data;
							nextPageNode = nextPageNode.next;
						}
					}
				}
			}
		}
		//if subgraph is not partitioned
		else {
			//if not partitioned directly write on page and return
			//                page.setData(((ByteArrayOutputStream) baos).toByteArray());
			page.setData(bb.array());
		}

		return nodeList;
	}

	private List<byte[]> partitionByteArray(byte[] array) {
		LinkedList<byte[]> balist = new LinkedList<byte[]>();
		byte[] new_arr = null;

		for (int i = 0; i < Math.ceil(array.length/(double)MAX_BLOCK_SIZE); i++) {
			int arraysize = array.length - (i * MAX_BLOCK_SIZE) > MAX_BLOCK_SIZE ? MAX_BLOCK_SIZE : array.length - (i * MAX_BLOCK_SIZE);
			new_arr = new byte[arraysize];

			//			System.arraycopy(array, i, current, 0, arraysize);
			System.arraycopy(array, i * MAX_BLOCK_SIZE, new_arr, 0, arraysize);

			balist.add(new_arr);
		}

		return balist;
	}

	public boolean removeEdge(Node src, Node dest){
		int bytesize = 0;

		//first remove the property of the edge
		g.removePropertyObject(new Edge(src,dest));

		//if src and/or dest is in this subgraph
		if(nodeMap.containsKey(src.getId())){
			if(outgoingEdgeMap.containsKey(src.getId())){
				for (Iterator<Edge> it = outgoingEdgeMap.get(src.getId()).iterator(); it.hasNext();) {
					Edge e = it.next();
					if(e.getDestinationId() == dest.getId()){
						it.remove();
						bytesize += edgeCost;
					}
				}
				if(outgoingEdgeMap.get(src.getId()).isEmpty()){
					bytesize += edgeInitCost;
				}
			}
		}

		if(nodeMap.containsKey(dest.getId())){
			if(incomingEdgeMap.containsKey(dest.getId())){
				for (Iterator<Edge> it = incomingEdgeMap.get(dest.getId()).iterator(); it.hasNext();) {
					Edge e = it.next();
					if(e.getSourceId() == src.getId()){
						it.remove();
						bytesize += edgeCost;
					}
				}
				if(incomingEdgeMap.get(dest.getId()).isEmpty()){
					bytesize += edgeInitCost;
				}
			}
		}

		decByteCount(bytesize);
		return (bytesize != 0);
	}

	public int removeProperty(Edge edge){
		int bytesize = 0;

		if(g.hasProperty(edge)){
			bytesize += g.getPropertyObject(edge).getObjectSize();
			decByteCount(bytesize);
		}

		return bytesize;
	}

	public int removeProperty(Node node){
		int bytesize = 0;

		if(g.hasProperty(node)){
			bytesize += g.getPropertyObject(node).getObjectSize();
			decByteCount(bytesize);
		}

		return bytesize;
	}

	public void removeNode(Node node) {
		int bytesize = 0;
		LinkedList<Edge> removeEdgeList = new LinkedList<Edge>();

		bytesize += nodeCost;

		if(incomingEdgeMap.containsKey(node.getId())){
			//if incident nodes of incoming edges are in the same subgraph then remove the edge from edgeMap
			for (Iterator<Edge> it = incomingEdgeMap.get(node.getId()).iterator(); it.hasNext();) {
				Edge e = it.next();
				removeEdgeList.add(e);
			}
		}

		if(outgoingEdgeMap.containsKey(node.getId())){
			//if incident nodes of outgoing edges are in the same subgraph then remove the edge form edgeMap
			for (Iterator<Edge> it = outgoingEdgeMap.get(node.getId()).iterator(); it.hasNext();) {
				Edge e = it.next();
				removeEdgeList.add(e);
			}
		}

		for (Iterator<Edge> it = removeEdgeList.iterator(); it.hasNext();) {
			Edge e = it.next();

			g.remove(e);
		}

		//erase the incoming edges of the node
		incomingEdgeMap.remove(node.getId());
		//erase the outgoing edges of the node
		outgoingEdgeMap.remove(node.getId());

		//erase node from nodeMap
		nodeMap.remove(node.getId());

		decByteCount(bytesize);
	}

	public void decByteCount(int x){
		if(!isDirty())
			setDirty(true);

		//issue access to the page in the buffer
		BufferManager.getInstance().issueAccessToBuffer(pageid);

		//TODO: subgraph merge or deletion
		bytecount -= x;
	}

	public void incByteCount(int x){
		if(!isDirty())
			setDirty(true);

		final int MAX_BLOCK_SIZE = GraphManager.getInstance().getBufferConfiguration().getMaxBlockSize();

		//issue access to the page in the buffer
		BufferManager.getInstance().issueAccessToBuffer(pageid);


		//if is not partitioned split the subgraph
		if(!partitioned){
			bytecount += x;

			if(getTotalSize() > MAX_BLOCK_SIZE){

				//if there is only one node and size is more than limit set partitioned
				if(nodeMap.size() == 1){
					setPartitioned(true);
					//				    log.trace("Split cannot be initiated, marked as partitioned :" + this + "(size" + bytecount + ")");	bytecount += x;

					//if there is no adjacent pages
					if(PageManager.getInstance().getPage(pageid).getAdjacentPagesList().getSize()  == 1){
						//create a new page for expansion in the node
						Page newpage = PageManager.getInstance().createNewPartitionedPage(g.getId(), sgid, PageManager.getInstance().getPage(pageid).getAdjacentPagesList());
						newpage.setPartitioned(true);

						//set previous page as partitioned.
						PageManager.getInstance().getPage(pageid).setPartitioned(true);
					}
				}
				else {
					//				    log.trace("<<<<<<<Split initiated for " + this + "(size" + bytecount + ") with " + GraphManager.getInstance().getSubgraphSplitPolicy().getClass().getName());
					//				    log.info("<<<<<<<Split initiated for " + this + "(size" + bytecount + ") with " + GraphManager.getInstance().getSubgraphSplitPolicy().getClass().getName());
					//					long start = System.currentTimeMillis();

					//split subgraph
					//					List<SubGraph> splittedSubgraphs = GraphManager.getInstance().getSubgraphSplitPolicy().split(g, this);
					GraphManager.getInstance().getSubgraphSplitPolicy().split(g, this);

					//					start = System.currentTimeMillis() - start;
					//				    log.trace(">>>>>>>Subgraph split finished: " + splittedSubgraphs);
					//				    log.info(">>>>>>>(" + start + "ms)Subgraph split finished: " + splittedSubgraphs);
				}

			}
		}
		else { //if partitioned then add item to the subgraph without splitting
			bytecount += x;

			if(Math.ceil(((double) getTotalSize())/((double) MAX_BLOCK_SIZE)) > PageManager.getInstance().getPage(pageid).getAdjacentPagesList().getSize()){
				Page newpage = PageManager.getInstance().createNewPartitionedPage(g.getId(), sgid, PageManager.getInstance().getPage(pageid).getAdjacentPagesList());
				newpage.setPartitioned(true);
			}
		}
		//TODO: for partitioned subgraphs do not allow node addition to the graph
	}

	public boolean isDirty(){
		return dirty;
	}

	public void setDirty(boolean status){
		dirty = status;
		PageManager.getInstance().getPage(pageid).setDirty(status);
	}

	public boolean isPartitioned(){
		return partitioned;
	}

	public void setPartitioned(boolean status){
		partitioned = status;
	}
	//from db
	public Node addNodeFromDb(Node node){
		nodeMap.put(node.getId(), node);
		bytecount += nodeCost;

		return node;
	}

	public void addOutgoingEdgeFromDb(Edge edge){
		long srcId = edge.getSourceId();
		int edgesize = 0;

		if(!g.edgeSet.contains(edge)){
			g.edgeSet.add(edge);
		}

		// if the node is in outgoingEdgeMap
		if(outgoingEdgeMap.containsKey(srcId) && !outgoingEdgeMap.get(srcId).isEmpty()){

			// directly add to the list
			outgoingEdgeMap.get(srcId).add(edge);

			edgesize += edgeCost;
		}
		else {

			// create a new edge list for the node
			List<Edge> newEdgeList = new LinkedList<Edge>();
			newEdgeList.add(edge);

			// and add it to the outgoingEdgeMap
			outgoingEdgeMap.put(srcId,newEdgeList);

			//if it is the first edge of the node
			//add the initialization cost
			edgesize += edgeInitCost;
			edgesize += edgeCost;
		}

		bytecount += edgesize;
	}

	public void addIncomingEdgeFromDb(Edge edge){
		long destId = edge.getDestinationId();
		int edgesize = 0;

		if(!g.edgeSet.contains(edge)){
			g.edgeSet.add(edge);
		}

		// if the node is already in incomingEdgeMap
		if(incomingEdgeMap.containsKey(destId) && !incomingEdgeMap.get(destId).isEmpty()){

			// directly add to the list of edges
			incomingEdgeMap.get(destId).add(edge);

			edgesize += edgeCost;
		}
		else {
			// create a new edge list for the node
			List<Edge> newEdgeList = new LinkedList<Edge>();
			newEdgeList.add(edge);

			// and add it to the incomingEdgeMap
			incomingEdgeMap.put(destId, newEdgeList);

			//if it is the first edge of the node
			//add the initialization cost
			edgesize += edgeInitCost;
			edgesize += edgeCost;
		}

		bytecount += edgesize;
	}

	public void addPropertyCost(int propertyCost){
		bytecount += propertyCost;
	}

	public void remove() {
		g.remove(this);
	}

	public void swapDummyNode(Node node) {
		nodeMap.put(node.getId(), node);
	}

	public Edge findEdge(long src, long dest) {
		//if node already contains that edge dont add it
		if(outgoingEdgeMap.containsKey(src)){
			for(Iterator<Edge> it = outgoingEdgeMap.get(src).iterator(); it.hasNext();){
				Edge e = it.next();
				if(e.getDestinationId() == dest){
					return e;
				}
				//				if(e.getDestination().getId() == dest){
				//					return e;
				//				}
			}
		}

		return null;
	}

	public boolean isCross(Edge e) {
		return !(nodeMap.containsKey(e.getSourceId()) & nodeMap.containsKey(e.getDestinationId()));
	}

	@Override
	public int hashCode() {
		return new Integer(sgid).hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof SubGraph){
			SubGraph sg = (SubGraph) obj;
			return sgid == sg.getId();
		}
		return false;
	}

	@Override
	public String toString() {
		String res = new String("Subgraph" + sgid + "(Page" + pageid + ")(" + (partitioned == true ? "P" : "NP") + "Size" + bytecount + ")" + ":" +nodeMap.keySet());
		return res;
	}
}

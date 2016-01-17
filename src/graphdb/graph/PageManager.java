package graphdb.graph;

import graphdb.util.DoublyLinkedList;
import graphdb.util.Property;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

public class PageManager {
	private long pageIdCounter;
	private HashMap<Long, Page> pageMap;

	private static Logger log = Logger.getLogger(PageManager.class.getName());

	//	private static class ListOfByteArrays implements Enumeration<ByteArrayInputStream>{
	//		Iterator<Page> current;
	//
	//		public ListOfByteArrays(Iterator<Page> byteListIterator) {
	//			current = byteListIterator;
	//		}
	//
	//		@Override
	//		public boolean hasMoreElements() {
	//			return current.hasNext();
	//		}
	//
	//		@Override
	//		public ByteArrayInputStream nextElement() {
	//			ByteArrayInputStream bais = null;
	//
	//			if(!hasMoreElements())
	//				throw new NoSuchElementException("No more byte arrays.");
	//            else {
	//			    log.trace("Reading partitioned next page to graph.");
	//                Page nextElement = current.next();
	//
	//                if(nextElement.getData() == null){
	//                	throw new RuntimeException(nextElement + " is null.");
	//                }
	//
	//                bais = new ByteArrayInputStream(nextElement.getData());
	//			}
	//
	//			return bais;
	//		}
	//	}

	protected PageManager(){
		if(PageManagerLoader.INSTANCE != null){
			throw new IllegalStateException("Already instantiated");
		}

		log.info("PageManager created.");

		//initialize the pm fields
		initialize();
	}

	private static class PageManagerLoader {
		private static final PageManager INSTANCE = new PageManager();
	}

	public static synchronized PageManager getInstance(){
		return PageManagerLoader.INSTANCE;
	}

	public void initialize(){
		pageMap = new HashMap<Long, Page>();
		pageIdCounter = 0;

	}

	public Page createNewPage(int gid, int sgid){
		Page p = new Page(getNewPageId(), gid, sgid, null, null);
		pageMap.put(p.getId(), p);

		//		if(pageMap.containsKey(p.getId())){
		//		    log.info("Page created with createNewPage: " + p.getId());
		//		}

		BufferManager.getInstance().addPageToBuffer(p);

		return p;
	}

	public Page createNewPageWithSubgraph(int gid){
		long pageid = getNewPageId();
		SubGraph sg = ((SuperGraph) GraphManager.getInstance().getGraph(gid)).createSubgraph(pageid);

		Page p = new Page(pageid, gid, sg.getId(), null, null);
		pageMap.put(p.getId(), p);

		//		if(pageMap.containsKey(p.getId())){
		//		    log.info("Page created with createNewPageWithSubgraph : " + p.getId());
		//		}

		BufferManager.getInstance().addPageToBuffer(p);

		return p;
	}

	public Page createNewPartitionedPage(int gid, int sgid, DoublyLinkedList<Page> adjlist){
		Page p = new Page(getNewPageId(), gid, sgid, adjlist, null);
		pageMap.put(p.getId(), p);

		//		if(pageMap.containsKey(p.getId())){
		//		    log.info("Page created with createNewPartitionedPage : " + p.getId());
		//		}

		BufferManager.getInstance().addPageToBuffer(p);

		return p;
	}

	public Page createPage(long pid, int gid, int sgid, DoublyLinkedList<Page> adjlist, byte[] data){
		Page p = new Page(pid, gid, sgid, adjlist, data);
		pageMap.put(pid, p);

		//		log.info("Page created with createPage : " + p.getId());

		BufferManager.getInstance().addPageToBuffer(p);

		return p;
	}

	public Page getPage(long pageid){
		if(pageMap.containsKey(pageid)){
			return pageMap.get(pageid);
		}

		throw new NoSuchElementException("Page " + pageid + " doesn't exist.\n" + BufferManager.getInstance().buff + "\n"+ pageMap);
	}

	public synchronized long getNewPageId(){
		return pageIdCounter++;
	}

	public void setPageIdCount(long pageIdCount) {
		pageIdCounter = pageIdCount;
	}

	public long getPageIdCount() {
		return pageIdCounter;
	}

	public void bringNode(long nodeid, SuperGraph superg){
		log.debug("Bring node initiated for node " + nodeid);

		//requests buffer manager to bring the page from db
		int graphid = superg.getId();
		SubGraph sg = superg.getSubgraphOfNode(nodeid);
		List<Long> pageIdList = BufferManager.getInstance().getPageListFromDb(nodeid, graphid);

		log.trace("Requested page(s) from db : " + pageIdList);

		//single not overflowed page
		if(pageIdList.size() == 1){
			long pid = pageIdList.get(0); // get the only element

			if(sg == null) { //if the page is not mapped to a subgraph
				//create a new subgraph
				sg = superg.createSubgraph(pid);
			}

			Page p = createPage(pid, graphid, sg.getId(), null, null); //not partitioned

			BufferManager.getInstance().addReadRequest(p);

			//for debug
			BufferManager.getInstance().validateTime = System.currentTimeMillis();
			//for debug

			readPageToGraph(graphid,p);

			//for debug
			BufferManager.getInstance().validateTime = System.currentTimeMillis() - BufferManager.getInstance().validateTime;
			//for debug
		}
		else if(pageIdList.size() > 1){ // overflowed pages
			DoublyLinkedList<Page> adjpages = null;

			if(sg == null) { //if the page is not mapped to a subgraph
				//create a new subgraph
				sg = superg.createSubgraph(pageIdList.get(0));
			}

			for (Iterator<Long> it = pageIdList.iterator(); it.hasNext();) {
				long pid = it.next();

				//at the first page create it and get the adjlist
				if(adjpages == null){
					Page p = createPage(pid, graphid, sg.getId(), null, null); //partitioned
					adjpages = p.getAdjacentPagesList();
					p.setPartitioned(true);
				}
				else {
					Page p = createPage(pid, graphid, sg.getId(), adjpages, null); //partitioned
					p.setPartitioned(true);
				}

			}

			BufferManager.getInstance().addReadRequest(adjpages.getFirstElement());

			//for debug
			BufferManager.getInstance().validateTime = System.currentTimeMillis();
			//for debug

			readPageToGraph(graphid,adjpages.getFirstElement());

			//for debug
			BufferManager.getInstance().validateTime = System.currentTimeMillis()
					- BufferManager.getInstance().validateTime;
			//for debug
		}
	}

	public void readPageToGraph(int graphid, Page p){
		SuperGraph superg = (SuperGraph) GraphManager.getInstance().getGraph(graphid);
		List<Node> nodelist = new LinkedList<Node>(); //for debug and log purposes

		//for debugging
		long start = System.currentTimeMillis();
		ArrayList<Long> nodetimes = new ArrayList<Long>();

		ArrayList<Long> inedgetimes = new ArrayList<Long>();
		ArrayList<Long> maxinedgetimes = new ArrayList<Long>();

		ArrayList<Long> outedgetimes = new ArrayList<Long>();
		ArrayList<Long> maxoutedgetimes = new ArrayList<Long>();

		ArrayList<Integer> edges = new ArrayList<Integer>();
		ArrayList<Integer> crossedges = new ArrayList<Integer>();
		Edge maxInEdge = null;
		Edge maxOutEdge = null;

		//for debugging
		int totnumedges = 0;

		try {
			//			ListOfByteArrays loa = new ListOfByteArrays(p.getAdjacentPagesList().iterator());
			//			SequenceInputStream sis = new SequenceInputStream(loa);
			//			ObjectInputStream in = new ObjectInputStream(sis);
			byte[][] barrays = new byte[p.getAdjacentPagesList().getSize()][];


			int edgecnt = 0;
			int crossedgecnt = 0;

			int j = 0;
			for (Iterator<Page> it = p.getAdjacentPagesList().iterator(); it.hasNext();) {
				Page page = it.next();
				barrays[j++] = page.getData();
			}

			ByteBuffer bb = ByteBuffer.wrap(concatByteArrays(barrays));

			//read the graph id and look for the graph
			long currentNodeId = -1;
			Node currentNode = null;
			int descriptor = -1;
			int sgid = p.getSubgraphId();

			long nodestart = System.currentTimeMillis();

			while(descriptor != 0){
				//read the descriptor
				//				descriptor = in.readInt();
				descriptor = bb.getInt();

				if(descriptor == 1){ // node start
					if(currentNodeId != -1){
						nodetimes.add(System.currentTimeMillis() - nodestart);
						edges.add(edgecnt);
						crossedges.add(crossedgecnt);

						nodestart = System.currentTimeMillis();
						edgecnt = 0;
						crossedgecnt = 0;
					}
					//read the node id and create the node
					//					currentNodeId = in.readLong();
					currentNodeId = bb.getLong();
					currentNode = superg.addNodeFromDb(currentNodeId, sgid);

					//for debug
					nodelist.add(currentNode);

					continue; //move to next punctuation
				}
				else if(descriptor == 2){ // in edge list start
					//for debugging
					long edgeStart = System.currentTimeMillis();
					long maxtime = 0;
					//for debugging

					//get the in edge count in the page
					//					long edgeNum = in.readLong();
					long edgeNum = bb.getLong();

					//for debugging
					edgecnt += edgeNum;
					totnumedges += edgeNum;
					//for debugging

					for (long i = 0; i < edgeNum; i++) {
						//for debugging
						long singleEdgeStart = System.currentTimeMillis();
						//for debugging

						//superg.addEdge(in.readLong(), currentNodeId, sgid);
						Edge e = superg.addIncomingEdgeFromDb(bb.getLong(), currentNodeId, sgid);

						//for debugging
						//						long _src = e.getSourceId();
						//						int _sgid = superg.nodeIndex.get(_src);
						//						SubGraph _sgg = superg.getSubgraph(_sgid);
						//
						//						if(_sgg.nodeMap.containsKey(_src)){
						//							if(_sgg.nodeMap.get(_src).isDummy()){
						//								crossedgecnt++;
						//							}
						//						}
						//for debugging

						int propLength = bb.getInt();

						if(propLength != -1){
							byte[] propData = new byte[propLength];
							bb.get(propData,0,propLength);
							Property edgeprop = Property.readPropertyObject(propData);

							//if there is no property for this edge, then add the property object
							//							if(!superg.hasProperty(e)){
							//								superg.setPropertyObject(e, edgeprop);
							//							}
							superg.setPropertyObject(e, edgeprop, currentNodeId);
						}
						//for debugging
						long t = System.currentTimeMillis() - singleEdgeStart;
						if(t > maxtime){
							maxtime = t;
							maxInEdge = e;
						}
						//for debugging
					}

					//for debugging
					inedgetimes.add(System.currentTimeMillis() - edgeStart);
					maxinedgetimes.add(maxtime);
					//for debugging

					continue; //move to next punctuation
				}
				else if(descriptor == 3){ // out edge list start
					//for debugging
					long edgeStart = System.currentTimeMillis();
					long maxtime = 0;
					//for debugging

					//get the out edge count in the page
					//					long edgeNum = in.readLong();
					long edgeNum = bb.getLong();

					//for debugging
					edgecnt += edgeNum;
					totnumedges += edgeNum;
					//for debugging

					for (long i = 0; i < edgeNum; i++) {

						//for debugging
						long singleEdgeStart = System.currentTimeMillis();
						//for debugging

						//superg.addEdge(currentNodeId, in.readLong(), sgid);
						Edge e = superg.addOutgoingEdgeFromDb(currentNodeId, bb.getLong(), sgid);

						//for debugging
						//						long _dest = e.getDestinationId();
						//						int _sgid = superg.nodeIndex.get(_dest);
						//						SubGraph _sgg = superg.getSubgraph(_sgid);
						//
						//						if(_sgg.nodeMap.containsKey(_dest)){
						//							if(_sgg.nodeMap.get(_dest).isDummy()){
						//								crossedgecnt++;
						//							}
						//						}
						//for debugging

						int propLength = bb.getInt();

						if(propLength != -1){
							byte[] propData = new byte[propLength];
							bb.get(propData,0,propLength);
							Property edgeprop = Property.readPropertyObject(propData);

							//if there is no property for this edge, then add the property object
							//							if(!superg.hasProperty(e)){
							//								superg.setPropertyObject(e, edgeprop);
							//							}
							superg.setPropertyObject(e, edgeprop, currentNodeId);
						}

						//for debugging
						long t = System.currentTimeMillis() - singleEdgeStart;
						if(t > maxtime){
							maxtime = t;
							maxOutEdge = e;
						}
						//for debugging
					}

					//for debugging
					outedgetimes.add(System.currentTimeMillis() - edgeStart);
					maxoutedgetimes.add(maxtime);

					//for debugging

					continue; //move to next punctuation
				}
				else if(descriptor == 4){ // node property start
					int propLength = bb.getInt();

					if(propLength != -1){
						byte[] propData = new byte[propLength];
						bb.get(propData,0,propLength);

						//read property object and add as node property
						Property nodeprop = Property.readPropertyObject(propData);
						superg.setPropertyObject(currentNode, nodeprop);
					}

					continue; //move to next punctuation
				}
				else if(descriptor <= 0){ // end of page
					break;
				}
				else {
					System.err.println("Error in stream. There are more than needed variables in the stream.");
					break;
				}

			}

			superg.getSubgraph(p.getSubgraphId()).setDirty(false);

			//			log.trace("Node list loaded to SubGraph" + p.getSubgraphId() + "(page" + p.getId() + ") : " + nodelist);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}


		start = System.currentTimeMillis() - start;
		if(start > 60){
			System.out.println();
			System.out.println("Num nodes : " + nodetimes.size());
			System.out.println("in edge times : " + inedgetimes);
			System.out.println("max in edge times : " + maxinedgetimes);
			System.out.println(maxInEdge);
			System.out.println("out edge times : " + outedgetimes);
			System.out.println("max out edge times : " + maxoutedgetimes);
			System.out.println(maxOutEdge);
			System.out.println("Num edges : " + totnumedges);
			System.out.println("Node times : " + nodetimes);
			System.out.println("edge nums : " + edges);
			System.out.println();
		}
	}

	public List<Long> invalidatePage(Page page) {
		List<Long> nodeList = null;

		//		log.trace(page + " is requested to be invalidated.");

		//get subgraph
		SuperGraph superg = (SuperGraph) GraphManager.getInstance().getGraph(page.getGraphId());
		SubGraph subg = superg.getSubgraph(page.getSubgraphId());

		//write subgraph into its page(s)
		nodeList = subg.writeSubgraphToPage();

		//remove nodes in invalidated block from index table
		for (Long nid : nodeList) {
			superg.nodeIndex.remove(nid);
		}

		//remove the subgraph from graph
		subg.remove();

		return nodeList;
	}

	//invalidate page for non dirty pages
	public void removeNodesOfPage(Page page) {

		//		log.trace(page + " is requested to be removed from buffer. Node list is being removed from graph.");

		//get subgraph
		SuperGraph superg = (SuperGraph) GraphManager.getInstance().getGraph(page.getGraphId());
		SubGraph subg = superg.getSubgraph(page.getSubgraphId());

		//remove nodes in invalidated block from index table
		for (Node n : subg.nodeMap.values()) {
			//set dummy for further usage
			n.setDummy(true);
			superg.nodeIndex.remove(n.getId());
		}

		//remove the subgraph from graph
		subg.remove();
	}

	private byte[] concatByteArrays(byte[]...arrays) {
		// Determine the length of the result array
		int totalLength = 0;
		for (int i = 0; i < arrays.length; i++)
		{
			totalLength += arrays[i].length;
		}

		// create the result array
		byte[] result = new byte[totalLength];

		// copy the source arrays into the result array
		int currentIndex = 0;
		for (int i = 0; i < arrays.length; i++)
		{
			System.arraycopy(arrays[i], 0, result, currentIndex, arrays[i].length);
			currentIndex += arrays[i].length;

			//TODO: uncomment and test the next line
			//	        arrays[i]arrays = null;
		}

		return result;
	}

	public void remove(Page p) {
		for (Iterator<Page> it = p.getAdjacentPagesList().iterator(); it.hasNext();) {
			Page page = it.next();
			pageMap.remove(page.getId());
		}
	}
}

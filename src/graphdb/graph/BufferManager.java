package graphdb.graph;

import graphdb.connector.LevelDBConnectorJava;
import graphdb.util.DoublyLinkedList;
import graphdb.util.Property;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;


public class BufferManager {
	private static Logger log = Logger.getLogger(BufferManager.class.getName());
	private DatabaseConnector db = null;

	public int bytesWritten;
	public int bytesRead;

	public long writeTime;
	public long readTime;

	public long invalidateTime;
	public long validateTime;

	private LinkedList<Page> readRequests;
	public DoublyLinkedList<Page> buff; //buffer for pages

	private HashMap<Long, DoublyLinkedList.Node<Page>> elementIndex;

	Property sysprops; //system properties
	BufferConfiguration buffConf;

	private BufferManager(){
		if(BufferManagerLoader.INSTANCE != null){
			throw new IllegalStateException("Already instantiated");
		}

		log.info("BufferManager created.");
		this.buffConf = new BufferConfiguration();

		this.bytesRead = 0;
		this.bytesWritten = 0;

		//initialize bm fields
		initialize();
	}

	private static class BufferManagerLoader {
		private static final BufferManager INSTANCE = new BufferManager();
	}

	public static synchronized BufferManager getInstance(){
		return BufferManagerLoader.INSTANCE;
	}

	public void initialize(){
		this.buff = new DoublyLinkedList<Page>();
		this.elementIndex = new HashMap<Long, DoublyLinkedList.Node<Page>>();
		this.readRequests = new LinkedList<Page>();


		if(db == null){
			//			for hbase
			//			HashMap<String,String> conf = new HashMap<String,String>();
			//			conf.put("hbase.master","localhost:60000");
			//			DatabaseConnector db = new HBaseConnector(conf);

			//			for leveldb
			//			DatabaseConnector db = new LevelDBConnectorJNI();
			DatabaseConnector db = new LevelDBConnectorJava("leveldb");

			this.db = db;
		}


		log.info("BufferManager initialized.");
		getSystemProperties();
	}

	public byte[] initGraphDb(){
		byte[] syspropdata = null;

		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			Property sysprop = new Property();

			sysprop.setProperty("pageIdCount", new Long(0));
			//write to output stream
			oos.writeObject(sysprop);

			syspropdata = baos.toByteArray();

			oos.close();
			baos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		//put initialized values into the database
		db.initGraphDb(syspropdata);

		log.debug("GraphDB initialized.");
		return syspropdata;
	}

	private void getSystemProperties() {
		// reads system properties from db

		byte[] syspropdata = db.getSystemProperties();

		//system has not been initialized yet initialize it
		if(syspropdata == null){
			syspropdata = initGraphDb();
		}

		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(syspropdata);
			ObjectInputStream ois = new ObjectInputStream(bais);

			sysprops = (Property) ois.readObject();

			//send page id count to page manager
			long pageIdCount = (Long) sysprops.getProperty("pageIdCount");
			PageManager.getInstance().setPageIdCount(pageIdCount);

			ois.close();
			bais.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		log.debug("System properties are loaded.");
	}

	public void addReadRequest(Page reqPage){
		readRequests.add(reqPage);
		issueReadRequests();
	}

	private void issueReadRequests(){
		for (Iterator<Page> it = readRequests.iterator(); it.hasNext();) {
			//TODO: first page may not needed here iterator can be fetched only
			Page firstpage = it.next().getAdjacentPagesList().getFirstElement(); //get the first element in the list

			bytesRead = 0;
			readTime = System.currentTimeMillis();

			//go over the adj page list of the requested page and bring the pages
			for (Iterator<Page> it2 = firstpage.getAdjacentPagesList().iterator(); it2.hasNext();) {
				Page p = it2.next();

				//go and read the page from database to the buffer.
				byte[] pagedata = db.getBlock(p.getId());

				bytesRead += pagedata.length;

				//set its data
				if(pagedata != null){
					p.setData(pagedata);
				}
			}
			readTime = System.currentTimeMillis() - readTime;

			it.remove();
		}

	}

	private void addToBuffer(Page p){
		if(!elementIndex.containsKey(p.getId())){
			log.debug("Page "+ p.getId() + " is requested to be added to buffer.");

			//if buffer is not full prepend to the list
			if(buff.getSize() < buffConf.getMaxBufferSize()){
				log.debug("Page "+ p.getId() + " is added to buffer.");

				DoublyLinkedList.Node<Page> element = buff.prependElement(p);
				elementIndex.put(p.getId(), element);
			}
			else {
				log.debug("Buffer is full. Initiating LRU page action.");
				//get rid of the LRU page
				writeLruPageToDb();

				//try to add p again
				addToBuffer(p);
			}
		}
	}

	public void writeLruPageToDb(){
		//		DoublyLinkedList.Node<Page> lastListNode = buff.getLastNode();

		//		//if the adjacent block list is as big as the buffer
		//		if(lastListNode.data.isPartitioned() && lastListNode.data.getAdjacentPagesList().getSize() == buff.getSize()){
		//			throw new RuntimeException("Buffer size is not enough to handle this graph. "
		//					+ "Please set the buffer size more than " + buff.getSize());
		//		}

		//if the last element in the buffer is first element's adj block leave it alone
		//try to find a new block to write on disk
		//		while(buff.getSize() != 1 && lastListNode.data.getAdjacentPagesList().getFirstElement() == buff.getFirstElement()){
		//			lastListNode = buff.getLastNode().prev;
		//
		//			if(lastListNode == buff.getFirstNode()){
		//			    throw new RuntimeException("Error: Got to the beginning of the buffer. "
		//			    		+ "Buffer size may not be enough.");
		//			}
		//		}

		//		Page lastPageAccSubgraph = null;
		//		LinkedList<Integer> sgbuff = new LinkedList<Integer>();
		//		for (Iterator<Page> it = buff.iterator(); it.hasNext();) {
		//			Page p = it.next();
		//			boolean contains = false;
		//
		//			for (Iterator<Integer> it2 = sgbuff.iterator(); it2.hasNext();) {
		//			    int sg = it2.next();
		//
		//				if(sg == p.getSubgraphId()){
		//					contains = true;
		//				}
		//			}
		//
		//			if(!contains){
		//				sgbuff.add(p.getSubgraphId());
		//				lastPageAccSubgraph = p;
		//			}
		//		}

		//performance improvement to above O(n^2) code
		Page lastPageAccSubgraph = null;
		HashSet<Integer> sgInBuff = new HashSet<Integer>();

		for (Iterator<Page> it = buff.iterator(); it.hasNext();) {
			Page p = it.next();

			if(!sgInBuff.contains(p.getSubgraphId())){
				lastPageAccSubgraph = p;
				sgInBuff.add(p.getSubgraphId());
			}

		}


		//		Page lastpage = lastListNode.data.getAdjacentPagesList().getFirstElement();
		Page lastpage = lastPageAccSubgraph.getAdjacentPagesList().getFirstElement();
		int graphid = lastpage.getGraphId();

		if(lastpage.getAdjacentPagesList().getSize() == buffConf.getMaxBufferSize()){
			throw new RuntimeException("Buffer size is not enough to handle this graph. "
					+ "Please set the buffer size more than " + buff.getSize());
		}

		if(lastpage.isDirty()){
			log.trace("Buffer's last state before LRU :" + buff);

			if(((SuperGraph) GraphManager.getInstance().getGraph(graphid)).getSubgraph(lastpage.getSubgraphId()) != null){
				//read subgraph into page/s
				invalidateTime = System.currentTimeMillis();
				List<Long> nodeList = PageManager.getInstance().invalidatePage(lastpage);
				invalidateTime = System.currentTimeMillis() - invalidateTime;

				List<Long> pageList = new LinkedList<Long>();


				log.trace("Node list being written to db: " + nodeList);

				//if nodelist is empty don't write anything on db
				if(!nodeList.isEmpty()){
					//if the page is dirty then write it to the storage
					bytesWritten = 0;
					writeTime = System.currentTimeMillis();

					for (Iterator<Page> it = lastpage.getAdjacentPagesList().iterator(); it.hasNext();) {
						Page page = it.next();

						if(page.getData() != null){
							pageList.add(page.getId());

							bytesWritten += page.getData().length;

							//write the page into database
							db.writeBlock(page.getId(), page.getData());
						}
						log.debug(page + " is written to db.");

						// remove the page from buffer
						buff.removeNode(elementIndex.get(page.getId()));;

						//remove from the page -> buffer-node index
						elementIndex.remove(page.getId());
					}
					writeTime = System.currentTimeMillis() - writeTime;

					log.trace("Page list written to db: " + pageList);
					writeNodeBlockList(nodeList, pageList, graphid);
				}
			}
			else {
				log.info("$$$$$$$$$$$$$$$$$$$$$$$$$ Subgraph is null : " + lastpage.getSubgraphId());

				throw new RuntimeException("Subgraph of " + lastpage + "is null. This shouldn't happen subgraph list : " + ((SuperGraph) GraphManager.getInstance().getGraph(graphid)).getSubgraphMap());
				//			    // remove the page from buffer
				//			    buff.removeNode(elementIndex.get(lastpage.getId()));;
				//
				//			    //remove from the page -> buffer-node index
				//			    elementIndex.remove(lastpage.getId());
			}

		}
		else {
			log.trace("No change in " + lastpage + ", nothing will be written to database.");
			log.trace("Buffer's last state before LRU :" + buff);

			PageManager.getInstance(). removeNodesOfPage(lastpage);

			//remove all adjacent pages from buffer
			for (Iterator<Page> it = lastpage.getAdjacentPagesList().iterator(); it.hasNext();) {
				Page page = it.next();

				// remove the page from buffer
				buff.removeNode(elementIndex.get(page.getId()));;

				//remove from the page -> buffer-node index
				elementIndex.remove(page.getId());
			}
		}

		//remove the pm index/es of page/s
		PageManager.getInstance().remove(lastpage);
	}

	public synchronized void issueAccessToBuffer(long pageid){
		Page p = PageManager.getInstance().getPage(pageid);
		if(p == null){
			throw new NoSuchElementException("Page doesn't exist :"+pageid);
		}
		buff.moveNodeToFront(elementIndex.get(p.getAdjacentPagesList().getFirstElement().getId()));

		log.trace("Access to " + pageid + " is issued.");
	}

	public synchronized void issueAccessToBuffer(Page p){
		buff.moveNodeToFront(elementIndex.get(p.getAdjacentPagesList().getFirstElement().getId()));
	}

	public synchronized void flushBuffer(){
		log.debug("Buffer flush initiated.");
		//write all elements in the buffer to the database
		while(!buff.isEmpty()){
			writeLruPageToDb();
		}
	}

	public synchronized HashMap<Integer, SuperGraph> getGraphList(){
		HashMap<Integer, byte[]> rawGraphList = null;
		HashMap<Integer, SuperGraph> gmap = new HashMap<Integer, SuperGraph>();
		int maxGraphId = 0; // min graph id can only be 1 it is automatically increased in the first usage

		rawGraphList = db.getGraphList();


		log.debug("Graph list from db is being loaded.");
		for (Iterator<Integer> it = rawGraphList.keySet().iterator(); it.hasNext();) {
			int key = it.next();

			try {
				ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(rawGraphList.get(key)));

				if(key != 0) {
					Property gprops = (Property) ois.readObject();

					long nodeIdCnt = (Long) gprops.getProperty("nodeIdCount");
					long nodeCnt = (Long) gprops.getProperty("nodeCount");
					String graphName = (String) gprops.getProperty("graphName");

					log.debug("Graph " + key + " = nodeIdCnt:" + nodeIdCnt + ", nodeCnt:" + nodeCnt);

					//					SuperGraph tempg = GraphManager.getInstance().createSuperGraph(key, nodeIdCnt, edgeIdCnt, nodeCnt);

					SuperGraph tempg = new SuperGraph(key, graphName, nodeIdCnt, nodeCnt);

					maxGraphId = key > maxGraphId ? key : maxGraphId;

					gmap.put(key, tempg);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}

		GraphManager.getInstance().setGraphIdCount(maxGraphId);

		return gmap;
	}

	public void writeGraphList(){
		HashMap<Integer, byte[]> rawglist = new HashMap<Integer, byte[]>();

		//write sysprop
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			Property sysprop = new Property();

			sysprop.setProperty("pageIdCount", new Long(PageManager.getInstance().getPageIdCount()));
			//write to output stream
			oos.writeObject(sysprop);


			//put into rawglist
			rawglist.put(0, baos.toByteArray());

			oos.close();
			baos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		//now write graphs
		Iterable<Graph> iter = GraphManager.getInstance().getGraphList();
		for(Graph g: iter) {
			//write graphs
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos);
				Property gprops = new Property();
				SuperGraph tempsuperg = (SuperGraph) g;

				gprops.setProperty("nodeIdCount", new Long(tempsuperg.getNodeIdCount()));
				gprops.setProperty("nodeCount", new Long(tempsuperg.getNumNodes()));
				gprops.setProperty("graphName", tempsuperg.getName());

				//write to output stream
				oos.writeObject(gprops);

				//put into rawglist
				rawglist.put(g.getId(), baos.toByteArray());

			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		//send to the db
		db.writeGraphList(rawglist);
	}

	public List<Long> getPageListFromDb(long nodeid, int graphid) {
		byte[] rawdata = db.getBlockList(nodeid, graphid);
		List<Long> blocklist = new LinkedList<Long>();

		try {
			ByteArrayInputStream dais = new ByteArrayInputStream(rawdata);
			ObjectInputStream ois = new ObjectInputStream(dais);

			int elemNum = ois.readInt();

			for (int i = 0; i < elemNum; i++) {
				blocklist.add(ois.readLong());
			}

			ois.close();
			dais.close();
		} catch (IOException e) {
			e.printStackTrace();
		}


		return blocklist;
	}

	public List<Long> getAllNodeList(int graphid){
		return db.getNodeList(graphid);
	}

	public void addPageToBuffer(Page p) {
		addToBuffer(p);
	}

	public void writeNodeBlockList(List<Long> nodeList, List<Long> pageList, int graphid){
		//writeNodeBlockList
		byte[] rawPageListData = null;

		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);

			//write block list size first
			oos.writeInt(pageList.size());

			//turn page list into byte array
			for (Iterator<Long> it = pageList.iterator(); it.hasNext();) {
				long pid = it.next();

				//write each element's id
				oos.writeLong(pid);
			}

			oos.close();
			baos.close();

			rawPageListData = baos.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		}

		db.writeNodeBlockList(nodeList,rawPageListData, graphid);

	}

	public Iterator<Long> getAllNodes(int graphid) {
		return db.getNodeList2(graphid);
	}

	public BufferConfiguration getBufferConf(){
		return buffConf;
	}

	public void changeDB(String dbName) {
		log.info("Changing database to " + dbName);

		//write the graph list to the db
		writeGraphList();

		//write all subgraphs on the memory to the db.
		flushBuffer();

		//close the database;
		db.closeDb();

		this.db = new LevelDBConnectorJava(dbName);

		this.buff = new DoublyLinkedList<Page>();
		this.elementIndex = new HashMap<Long, DoublyLinkedList.Node<Page>>();
		this.readRequests = new LinkedList<Page>();

		log.info("BufferManager reinitialized.");
		getSystemProperties();
	}

	public void shutdown() {
		log.info("Shutting down...");
		//write the graph list to the db
		writeGraphList();

		//write all subgraphs on the memory to the db.
		flushBuffer();

		//close the database;
		db.closeDb();

		log.info("Done.");
	}
}

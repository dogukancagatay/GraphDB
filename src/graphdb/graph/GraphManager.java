package graphdb.graph;

import graphdb.policy.SubgraphFirstAvailableSelect;
import graphdb.policy.SubgraphHalfSplit;
import graphdb.util.Property;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

public class GraphManager {
	private int graphIdCount;
	private static boolean initialized = false;
	private static SubGraphSplitPolicy splitPol;
	private static SubGraphSelectPolicy selectPol;

	private static HashMap<Integer, SuperGraph> gmap; // graphid -> graph

	private static Logger log = Logger.getLogger(GraphManager.class.getName());

	private GraphManager(){
		if(GraphManagerLoader.INSTANCE != null){
			throw new IllegalStateException("Already instantiated");
		}

		graphIdCount = 0; //starts from 1 but it is increased in the first time.
		gmap = null;

		log.info("GraphManager created.");

	}

	private static class GraphManagerLoader {
		private static final GraphManager INSTANCE = new GraphManager();
	}

	public static synchronized GraphManager getInstance(){
		//get graph list from buffer manager
		if(!initialized){
			initialized = true;
			gmap = BufferManager.getInstance().getGraphList();

			splitPol = new SubgraphHalfSplit();
			selectPol = new SubgraphFirstAvailableSelect();
		}
		return GraphManagerLoader.INSTANCE;
	}

	//user API call
	public Graph createGraph(){
		return createSuperGraph().getGraph();
	}

	public Graph createGraph(String graphName){
		return createSuperGraph(graphName).getGraph();
	}

	// graph copy function
	public Graph createGraph(Graph g){
		SuperGraph oldg = (SuperGraph) g;
		SuperGraph newg = createSuperGraph();
		newg.nodeCounter = oldg.nodeCounter;
		newg.nodeIdCounter = oldg.nodeIdCounter;

		for(Node n : g.getNodes()){
			Node nn = newg.addNode(n.getId());

			if(oldg.nodeProps.containsKey(n.getId())){
				Property oldProp = oldg.nodeProps.get(n.getId());
				Property newProp = new Property(oldProp);

				newg.nodeProps.put(nn.getId(), newProp);
			}
		}

		for(Node n : g.getNodes()){
			for(Edge oe : n.getEdges(EdgeDirection.OUT)){
				Node nn = newg.getNode(n.getId());
				Edge ne = nn.addEdge(newg.getNode(oe.getDestinationId()));

				if(oldg.edgeProps.containsKey(oe)){
					Property oldProp = oldg.edgeProps.get(oe);
					Property newProp = new Property(oldProp);

					newg.edgeProps.put(ne, newProp);
				}
			}
		}

		return newg.getGraph();
	}

	private SuperGraph createSuperGraph(){
		return createSuperGraph(String.valueOf(graphIdCount+1));
	}

	private SuperGraph createSuperGraph(String graphName){
		SuperGraph superg = null;

		changeDB(graphName);

		try {
			superg = (SuperGraph)GraphManager.getInstance().getGraph(graphName);
			//            throw new RuntimeException("Another graph with the same name (" + graphName + ") exists. Graph name identifiers must be unique.");
		} catch (NoSuchElementException e){
			int gid = ++graphIdCount;
			superg = new SuperGraph(gid,graphName,0,0);

			//add it to the graph index
			gmap.put(gid, superg);
		}

		return superg;
	}

	public void setGraphIdCount(int idCount){
		graphIdCount = idCount;
	}

	public SuperGraph createSuperGraph(int id, String graphName, long nodeIdCounterValue, long nodeCounterValue){
		//create the super graph with specified id
		SuperGraph superg = createSuperGraph(id, graphName);

		//fill in the counter values
		if(gmap.containsKey(id)){
			superg.setNodeIdCounter(nodeIdCounterValue);
			superg.setNodeCounter(nodeCounterValue);
		}

		return superg;
	}

	private SuperGraph createSuperGraph(int id, String graphName){
		//check if the graph exists
		if(gmap.containsKey(id)){
			return gmap.get(id);
		}

		SuperGraph superg = new SuperGraph(id,graphName,0,0);
		gmap.put(id, superg);

		return superg;
	}

	//user API call
	public Graph getGraph(int gid){
		if(gmap.get(gid) == null)
			throw new NoSuchElementException("There is no graph with id " + gid);

		return gmap.get(gid).getGraph();
	}

	public Graph getGraph(String graphName){
		SuperGraph g = null;

		changeDB(graphName);

		for (Iterator<SuperGraph> it = gmap.values().iterator(); it.hasNext();) {
			SuperGraph tempg = it.next();
			if(graphName.equals(tempg.getName())){
				g = tempg;
			}
		}

		if(g == null)
			throw new NoSuchElementException("There is no graph with name " + graphName);

		return g.getGraph();
	}

	//user API call
	public Iterable<Graph> getGraphList() {
		return new Iterable<Graph> () {

			@Override
			public Iterator<Graph> iterator() {
				//gmap is in the super graph type
				HashMap<Integer, Graph> graphmap = new HashMap<Integer, Graph>(gmap);
				return graphmap.values().iterator();
			}
		};
	}

	public HashMap<Integer,SuperGraph> getGmap(){
		return gmap;
	}

	public List<Long> getNodeList(int graphid){
		if(gmap.containsKey(graphid)){
			return BufferManager.getInstance().getAllNodeList(graphid);
		}
		return null;
	}

	//	public Iterator<Long> getNodesOnDb(int graphid){
	//
	//		if(gmap.containsKey(graphid)){
	//			return BufferManager.getInstance().getAllNodes(graphid);
	//		}
	//		return null;
	//	}

	public Iterator<Long> getNodes(int graphid){
		if(gmap.containsKey(graphid)){
			return BufferManager.getInstance().getAllNodes(graphid);
		}
		return null;
	}

	public void bringNode(long nodeid, int graphid) {
		//send super graph to the page manager
		PageManager.getInstance().bringNode(nodeid, gmap.get(graphid));
	}

	public SubGraphSplitPolicy getSubgraphSplitPolicy(){
		return splitPol;
	}

	public void setSubgraphSplitPolicy(SubGraphSplitPolicy pol){
		splitPol = pol;
	}

	public SubGraphSelectPolicy getSubgraphSelectPolicy(){
		return selectPol;
	}

	public void setSubgraphSelectPolicy(SubGraphSelectPolicy pol){
		selectPol = pol;
	}

	public void changeDB(String dbName){
		log.info("Change Database initiated.");

		//create new database with dbName
		BufferManager.getInstance().changeDB(dbName);

		gmap = BufferManager.getInstance().getGraphList();

	}

	public void deleteEverything(){
		log.info("Delete Everything is initiated.");

		//deletes everything in the database and reinitializes it in the database
		BufferManager.getInstance().initGraphDb();

		// reinitialize the gm, pm, and bm
		PageManager.getInstance().initialize(); //initialize pm
		BufferManager.getInstance().initialize(); // get new sysprops and set it on pm

		gmap = BufferManager.getInstance().getGraphList();

	}

	public void shutdown(){
		//writes sysprops, graphlists, flushes the buffer in the subgraphs and closes the database
		BufferManager.getInstance().shutdown();
	}

	public BufferConfiguration getBufferConfiguration(){
		return BufferManager.getInstance().getBufferConf();
	}

	public void setBufferConfiguration(BufferConfiguration bc){
		BufferManager.getInstance().getBufferConf().setMaxBufferSize(bc.getMaxBufferSize());
		BufferManager.getInstance().getBufferConf().setMaxBlockSize(bc.getMaxBlockSize());
	}
}
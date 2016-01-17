package graphdb.policy;

import graphdb.graph.GraphManager;
import graphdb.graph.Node;
import graphdb.graph.PageManager;
import graphdb.graph.SubGraph;
import graphdb.graph.SubGraphSelectPolicy;
import graphdb.graph.SuperGraph;

import java.util.Iterator;

import org.apache.log4j.Logger;

public class SubgraphFirstAvailableSelect implements SubGraphSelectPolicy {
	private static Logger log = Logger.getLogger(SubgraphFirstAvailableSelect.class.getName());
	private static final int MAX_BLOCK_SIZE = GraphManager.getInstance().getBufferConfiguration().getMaxBlockSize();

	@Override
	public SubGraph select(Node n, SuperGraph g) {
		log.trace("Selecting first available subgraph for "+ n);
		
		int subgraphSize = g.getSubgraphMap().size();
		int nodeSize = SubGraph.nodeCost;
		
		if(subgraphSize == 0){
			log.trace("There are no subgraphs in the system so creating one.");
			//a brand new subgraph
			return g.getSubgraph(PageManager.getInstance().createNewPageWithSubgraph(g.getId()).getSubgraphId());
		}
		
		//when adding a new node
		if(n == null){
		    for (Iterator<SubGraph> it = g.getSubgraphMap().values().iterator(); it.hasNext();) {
			    SubGraph sg = it.next();

			    //add node if subgraph is not partitioned
			    if(!sg.isPartitioned()){
				    if(MAX_BLOCK_SIZE - sg.getTotalSize() > nodeSize){
					    log.trace("Found one" + sg + " ");
						return sg;
				    }
			    }
		    }
		}
		else {
		    for (Iterator<SubGraph> it = g.getSubgraphMap().values().iterator(); it.hasNext();) {
			    SubGraph sg = it.next();

			    //add node if subgraph is not partitioned
			    if(!sg.isPartitioned()){
				    if(MAX_BLOCK_SIZE - sg.getTotalSize() > sg.getSize(n)){
				    	log.trace("Found one" + sg + " ");
						return sg;
				    }
			    }
		    }
		}
		
		//if there are no available subgraph create and return a new empty one
		return g.getSubgraph(PageManager.getInstance().createNewPageWithSubgraph(g.getId()).getSubgraphId());
	}

}

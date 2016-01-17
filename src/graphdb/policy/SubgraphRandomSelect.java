package graphdb.policy;

import graphdb.graph.Node;
import graphdb.graph.PageManager;
import graphdb.graph.SubGraph;
import graphdb.graph.SubGraphSelectPolicy;
import graphdb.graph.SuperGraph;

import java.util.Collection;
import java.util.Iterator;
import java.util.Random;

import org.apache.log4j.Logger;

public class SubgraphRandomSelect implements SubGraphSelectPolicy {
	private static Logger log = Logger.getLogger(SubgraphFirstAvailableSelect.class.getName());
//	private static final int MAX_BLOCK_SIZE = GraphManager.getInstance().getBufferConfiguration().getMaxBlockSize();

	@Override
	public SubGraph select(Node n, SuperGraph g) {
		Random ng = new Random(5);
		int subgraphSize = g.getSubgraphMap().size();
//		int nodeSize = SubGraph.nodeCost;

		log.trace("Selecting a random subgraph for "+ n);
		
		//TODO: getting real nodesize for not null n

		if(subgraphSize == 0){
			log.trace("There are no subgraphs in the system so creating one.");
			//a brand new subgraph
			return g.getSubgraph(PageManager.getInstance().createNewPageWithSubgraph(g.getId()).getSubgraphId());
		}
		
		Collection<SubGraph> sglist = g.getSubgraphMap().values(); 
		int rindex = ng.nextInt(subgraphSize);
		    
		int i = 0;
		for (Iterator<SubGraph> it = sglist.iterator(); it.hasNext();) {
			SubGraph sgid = it.next();

			if(i == rindex){
				//cannot select a partitioned subgraph
				if(!sgid.isPartitioned()){
					return sgid;
				}
				break;
			}
			i++;
		}

		
		return g.getSubgraph(PageManager.getInstance().createNewPageWithSubgraph(g.getId()).getSubgraphId());
		
//		throw new IllegalArgumentException("Subgraph selection is not done correctly.");
		//if something wrong happened return a new subgraph
//		return g.getSubgraph(PageManager.getInstance().createNewPage(g.getId()).getSubgraphId());
	}

}

package graphdb.policy;

import graphdb.graph.Edge;
import graphdb.graph.Node;
import graphdb.graph.Page;
import graphdb.graph.PageManager;
import graphdb.graph.SubGraph;
import graphdb.graph.SubGraphSplitPolicy;
import graphdb.graph.SuperGraph;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class SubgraphHalfSplit implements SubGraphSplitPolicy {

	@Override
	public List<SubGraph> split(SuperGraph superg, SubGraph oldsg) {
		List<SubGraph> sglist = new LinkedList<SubGraph>();

		//create a new subgraph and page pair
		Page newpage = PageManager.getInstance().createNewPage(superg.getId(), -1);
		SubGraph newsg = superg.createSubgraph(newpage.getId());
		newpage.setSubgraph(newsg.getId());
		newsg.setDirty(true);

		int oldsize = oldsg.nodeMap.size();
		int i = 0;

		//randomly split the nodes between two subgraphs
		for (Iterator<Node> it = oldsg.getNodes().iterator(); it.hasNext();) {
			Node n = it.next();
			if(i < (oldsize/2)){
				superg.moveNode(n, oldsg, newsg);

				//remove the node from old subgraph with iterator
				it.remove();
			}
			i++;
		}

		sglist.add(oldsg);
		sglist.add(newsg);

		return sglist;
	}

	@Override
	public void addEdgeAction(Edge e) {
	}

}

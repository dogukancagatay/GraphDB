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
import java.util.Random;

public class SubgraphRandomSplit implements SubGraphSplitPolicy {

	@Override
	public List<SubGraph> split(SuperGraph superg, SubGraph oldsg) {
		Random rg = new Random();
		List<SubGraph> sglist = new LinkedList<SubGraph>();

		//create a new subgraph and page pair
		Page newpage = PageManager.getInstance().createNewPage(superg.getId(), -1);
		SubGraph newsg = superg.createSubgraph(newpage.getId());
		newpage.setSubgraph(newsg.getId());
		newsg.setDirty(true);


		//randomly split the nodes between two subgraphs
		for (Iterator<Node> it = oldsg.nodeMap.values().iterator(); it.hasNext();) {
			Node n = it.next();
			if(rg.nextBoolean()){
				superg.moveNode(n, oldsg, newsg);
				//				System.out.println("Moved node " + n + " from " + oldsg.getId() + " to " + newsg.getId());

				//remove the node from old subgraph with iterator
				it.remove();
			}

		}

		sglist.add(oldsg);
		sglist.add(newsg);

		return sglist;
	}

	@Override
	public void addEdgeAction(Edge e) {
	}

}

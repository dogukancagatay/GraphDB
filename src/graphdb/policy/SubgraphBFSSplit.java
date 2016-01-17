package graphdb.policy;

import graphdb.graph.Edge;
import graphdb.graph.EdgeDirection;
import graphdb.graph.Node;
import graphdb.graph.Page;
import graphdb.graph.PageManager;
import graphdb.graph.SubGraph;
import graphdb.graph.SubGraphSplitPolicy;
import graphdb.graph.SuperGraph;
import graphdb.util.Pair;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.log4j.Logger;

public class SubgraphBFSSplit implements SubGraphSplitPolicy {
	private static Logger log = Logger.getLogger(SubgraphBFSSplit.class.getName());
	//	private static final int MAX_BLOCK_SIZE = GraphManager.getInstance().getBufferConfiguration().getMaxBlockSize();

	@Override
	public List<SubGraph> split(SuperGraph superg, SubGraph oldsg) {
		List<SubGraph> sglist = new LinkedList<SubGraph>();

		//		log.info("Split for " + oldsg);

		//create a new subgraph and page pair
		//		Page newpage = PageManager.getInstance().createNewPage(superg.getId(), -1);
		//		SubGraph newsg = superg.createSubgraph(newpage.getId());
		//		newpage.setSubgraph(newsg.getId());

		Page newpage = PageManager.getInstance().createNewPageWithSubgraph(superg.getId());
		SubGraph newsg = superg.getSubgraph(newpage.getSubgraphId());
		newsg.setDirty(true);

		int numNodesMoved = 0;
		int maxNodesToMove = oldsg.nodeMap.size()/2;
		while(numNodesMoved < maxNodesToMove){
			//BFS
			Queue<Pair<Node,Integer>> q = new LinkedList<Pair<Node,Integer>>();
			HashSet<Long> visited = new HashSet<Long>(); //hold visited node id
			int hopCount = 0;	// the level, hop count

			Node root = oldsg.nodeMap.values().iterator().next(); //get a random node
			q.add(new Pair<Node, Integer>(root,0));

			while(!q.isEmpty() && numNodesMoved < maxNodesToMove) {
				Pair<Node, Integer> p = q.peek();
				Node n = p.getLeft();
				hopCount = p.getRight();

				//				if(numNodesMoved % 1000 == 0){
				log.trace("BFS queue for iteration:" + q);
				log.trace("BFS queue size for iteration:" + q.size());
				log.trace("BFS processing node : "+n + " hopCount:" + hopCount);
				log.trace("BFS nodes moved : " + numNodesMoved);
				//				}

				// only add the neighborhood of n in the same subgraph
				for(Edge e : n.getEdges(EdgeDirection.OUT)){
					Long nodeid = e.getDestinationId();

					if(!visited.contains(nodeid) && oldsg.nodeMap.containsKey(nodeid)){
						Node u = e.getDestination();
						visited.add(nodeid);
						q.add(new Pair<Node,Integer>(u, hopCount+1));
					}
				}

				//move node
				superg.moveNode(n, oldsg, newsg);
				oldsg.nodeMap.remove(n.getId());
				numNodesMoved++;

				//remove n from queue
				q.poll();

				//if the newsg is partitioned and there are more nodes to move
				if (!q.isEmpty() &&
						numNodesMoved < maxNodesToMove &&
						newsg.isPartitioned()) {

					log.trace("Newly created, " + newsg + ", is full creating a new one.");

					//add the previous subgraph to subgraph list
					sglist.add(newsg);

					newpage = PageManager.getInstance().createNewPageWithSubgraph(superg.getId());
					newsg = superg.getSubgraph(newpage.getSubgraphId());
					newsg.setDirty(true);
				}
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

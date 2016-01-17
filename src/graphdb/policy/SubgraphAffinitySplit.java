package graphdb.policy;

import graphdb.graph.Edge;
import graphdb.graph.Node;
import graphdb.graph.SubGraph;
import graphdb.graph.SubGraphSplitPolicy;
import graphdb.graph.SuperGraph;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;


public class SubgraphAffinitySplit implements SubGraphSplitPolicy {
	private static Logger log = Logger.getLogger(SubgraphAffinitySplit.class.getName());
	SubGraphSplitPolicy defaultSplitPol;
	int outCrossEdgeCount;
	int inCrossEdgeCount;

	public SubgraphAffinitySplit(){
		defaultSplitPol = new SubgraphHalfSplit();
	}

	@Override
	public List<SubGraph> split(SuperGraph superg, SubGraph oldsg) {
		return defaultSplitPol.split(superg, oldsg);
	}

	@Override
	public void addEdgeAction(Edge e) {
		Node src = e.getSource();
		SuperGraph superg = (SuperGraph) src.getGraph();

		//for source node
		SubGraph oldsg = superg.getSubgraphOfNode(src.getId());
		SubGraph newsg = getAffSubGraph(src, oldsg, superg);

		log.trace("Source aff subgraph is decided: " + newsg);

		//move node to new subgraph
		if(newsg != null){
			superg.moveNode(src, oldsg, newsg);
			oldsg.nodeMap.remove(src.getId());
		}

		//for dest node
		Node dest = e.getDestination();
		oldsg = superg.getSubgraphOfNode(dest.getId());
		newsg = getAffSubGraph(dest, oldsg, superg);

		log.trace("Destination aff subgraph is decided: " + newsg);

		//move node to new subgraph
		if(newsg != null){
			superg.moveNode(dest, oldsg, newsg);
			oldsg.nodeMap.remove(dest.getId());
		}

	}


	private SubGraph getAffSubGraph(Node n, SubGraph sg, SuperGraph superg){
		//		int crossEdgeCount = 0;
		int innerCrossEdgeCount = 0; //edge count in node's subgraph
		int totalEdgeCount = 0;

		HashMap<SubGraph, Integer> crossEdgesInMem = new HashMap<SubGraph, Integer>();

		if(sg.outgoingEdgeMap.containsKey(n.getId())){
			totalEdgeCount += sg.outgoingEdgeMap.get(n.getId()).size();

			for(Edge oute : sg.outgoingEdgeMap.get(n.getId())){
				Long dest = oute.getDestinationId();

				//if the node is in memory
				//if subgraphs are different
				if(superg.nodeIndex.containsKey(dest) && superg.nodeIndex.get(dest) != sg.getId()){
					SubGraph destsg = superg.getSubgraph(superg.nodeIndex.get(dest));
					//					crossEdgeCount++;

					//increase cross edge counter
					if(crossEdgesInMem.containsKey(destsg)){
						crossEdgesInMem.put(destsg, crossEdgesInMem.get(destsg) + 1);
					}
					else {
						crossEdgesInMem.put(destsg, 1);
					}
				}
				else{
					innerCrossEdgeCount++;
				}
			}
		}

		if(sg.incomingEdgeMap.containsKey(n.getId())){
			totalEdgeCount += sg.incomingEdgeMap.get(n.getId()).size();

			//for in edges
			for(Edge ine : sg.incomingEdgeMap.get(n.getId())){
				Long src = ine.getSourceId();

				//if the node is in memory
				//if subgraphs are different
				if(superg.nodeIndex.containsKey(src) && superg.nodeIndex.get(src) != sg.getId()){
					SubGraph srcsg = superg.getSubgraph(superg.nodeIndex.get(src));
					//					crossEdgeCount++;

					//increase cross edge counter
					if(crossEdgesInMem.containsKey(srcsg)){
						crossEdgesInMem.put(srcsg, crossEdgesInMem.get(srcsg) + 1);
					}
					else {
						crossEdgesInMem.put(srcsg, 1);
					}
				}
				else{
					innerCrossEdgeCount++;
				}
			}
		}

		//calculate present subgraph's node affinity
		double selfAff = calcFormula(totalEdgeCount, innerCrossEdgeCount);

		SubGraph maxSg = null;
		double maxAff = Double.MIN_VALUE;
		double switchThreshold = 0.0;

		//take the max affinity of the cross subgraphs
		for(Iterator<SubGraph> it = crossEdgesInMem.keySet().iterator(); it.hasNext();){
			SubGraph crossSg = it.next();
			double aff = calcFormula(totalEdgeCount, crossEdgesInMem.get(crossSg));

			if(aff != 1.0 && maxAff < aff){
				maxAff = aff;
				maxSg = crossSg;
			}
		}

		//compare to the present subgraph
		if((maxAff - selfAff) > switchThreshold){
			if(maxSg != null && maxSg.equals(sg)){
				return null;
			}
			else{
				return maxSg;
			}
		}

		return null;
	}

	private double calcFormula(int totalEdgeCount, int sgCrossEdgeCount){
		//affinity formula
		double aff = 1.0;
		double factor = 1000;
		aff = 1 - Math.exp( ((double)totalEdgeCount - (double)sgCrossEdgeCount) / factor);

		return aff;
	}
}

package graphdb.algo;

import graphdb.graph.Graph;
import graphdb.graph.GraphQueryAlgorithm;

public class KCoreDecomposition implements GraphQueryAlgorithm{
	Graph g, newGraph = null;
	int k;
	public KCoreDecomposition(Graph g, int k) {
		this.g = g;
		this.k = k;
	}

	public Graph run(){
		//if newgraph is null initialize it
		if(newGraph == null){

		}

		return newGraph;
	}
	//	private Graph initialize(Graph g){
	//		Graph ng = GraphManager.getInstance().createGraph();
	//		ng.copyGraph(g);
	//	}
}

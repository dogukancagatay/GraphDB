package graphdb.algo;

import graphdb.graph.BufferConfiguration;
import graphdb.graph.Edge;
import graphdb.graph.EdgeDirection;
import graphdb.graph.Graph;
import graphdb.graph.GraphManager;
import graphdb.graph.GraphQueryAlgorithm;
import graphdb.graph.Node;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ClusteringCoefficient implements GraphQueryAlgorithm{
	private static Logger log = Logger.getLogger(ClusteringCoefficient.class.getName());
	private boolean isDirected;

	public double run(Graph g, boolean isDirected){
		double globalCC = 0.0;
		this.isDirected = isDirected;

		//calculate cc for each node
		for (Node n : g.getNodes()) {
			globalCC += ccOfNode(n);
		}

		log.trace("Clustering coefficient globalCC: " + globalCC);
		log.trace("Clustering number of nodes: " + g.getNumNodes());

		return globalCC / (double)g.getNumNodes(); // average of cc per node
	}

	private double ccOfNode(Node n){
		//cc = \frac{|\{e_{jk}: v_j,v_k \in N_i, e_{jk} \in E\}|}{k_i(k_i-1)}
		//for bidirectional cc*2

		LinkedList<Node> nhood = new LinkedList<Node>();
		int numEdges = 0;

		//add out edges
		for(Edge e : n.getEdges(EdgeDirection.OUT)){
			nhood.add(e.getDestination());
		}

		//if directed add in edges too
		if(isDirected){
			for(Edge e : n.getEdges(EdgeDirection.IN)){
				nhood.add(e.getDestination());
			}
		}

		log.trace("CC neighborhood for node" + n + " " + nhood);

		//if number one or no edge exist skip that node
		if(nhood.size() < 2){
			log.trace("CC calculation for node"+ n + " 0.0");
			return 0.0;
		}

		//How big is this nodes neighborhood
		double size = nhood.size() * (nhood.size()  - 1);

		//for each neighbor of n
		while(nhood.size() > 0){
			Node n1 = nhood.removeFirst();

			for (Iterator<Node> it = nhood.iterator(); it.hasNext();) {
				Node nn = it.next(); // next neighbor

				//if n1 is connected to nn increase
				for(Edge e : n1.getEdges(EdgeDirection.OUT)){
					if(e.getDestination().equals(nn))
						numEdges++;
				}

				//if g is directed
				if(isDirected){
					//if n1 is connected to nn increase
					for(Edge e : nn.getEdges(EdgeDirection.OUT)){
						if(e.getDestination().equals(n1))
							numEdges++;
						log.trace("CC node"+ n1 + " 's neighbor node" + e.getDestination() + " is in nhood of node" + n);
					}
				}
			}
		}

		if(!isDirected){
			numEdges *= 2;
		}

		log.trace("Clustering coefficient calculation for node"+ n +" " + ((double)numEdges/size));
		return (double) numEdges / size;
	}

	@SuppressWarnings("unused")
	private double ccOfNode2(Node n){
		HashSet<Node> nhood = new HashSet<Node>();
		int numEdges = 0;

		//add out edges
		for(Edge e : n.getEdges(EdgeDirection.OUT)){
			nhood.add(e.getDestination());
		}

		log.trace("CC neighborhood for node" + n + " " + nhood);

		//if number one or no edge exist skip that node
		if(nhood.size() < 2){
			log.trace("CC calculation for node"+ n + " 0.0");
			return 0.0;
		}

		//How big is this nodes neighborhood
		double possibleEdges = nhood.size() * (nhood.size()  - 1);
		log.trace("CC possible edges of node" + n + " "+ possibleEdges);

		//for each neighbor of neighbor of n
		for(Edge e : n.getEdges(EdgeDirection.OUT)){
			for(Edge ne : e.getDestination().getEdges(EdgeDirection.OUT)){
				if(nhood.contains(ne.getDestination())){
					log.trace("CC node"+ e.getDestination() + " 's neighbor node" + ne.getDestination() + " is in nhood of node"+n);
					numEdges++;
				}
			}
		}

		//		if(!isDirected){
		//			numEdges *= 2;
		//		}

		log.trace("Clustering coefficient calculation for node"+ n +" " + ((double)numEdges/possibleEdges));
		return (double) numEdges / possibleEdges;
	}

	public static void main(String[] args){
		Logger.getRootLogger().setLevel(Level.DEBUG);

		GraphManager gm = null;
		try{
			Node[] arr1 = new Node[4];
			Node[] arr2 = new Node[4];
			Node[] arr3 = new Node[7];

			gm = GraphManager.getInstance();
			BufferConfiguration bc = new BufferConfiguration(3,100);
			gm.setBufferConfiguration(bc);

			gm.deleteEverything();

			Graph g1 = gm.createGraph();
			Graph g2 = gm.createGraph();
			Graph g3 = gm.createGraph();

			System.out.println("Adding Nodes for Graph1");
			for (int i = 0; i < arr1.length; i++) {
				arr1[i] = g1.addNode();
			}

			System.out.println("List of Nodes for Graph 1");
			for(Node nd : g1.getNodes()){
				System.out.println("Node " + nd.getId());
			}

			System.out.println("Adding Edges for Graph 1");
			System.out.println("0 to 1");
			arr1[0].addEdge(arr1[1]);
			System.out.println("0 to 2");
			arr1[0].addEdge(arr1[2]);
			System.out.println("0 to 3");
			arr1[0].addEdge(arr1[3]);

			System.out.println("1 to 0");
			arr1[1].addEdge(arr1[0]);

			System.out.println("2 to 0");
			arr1[2].addEdge(arr1[0]);
			System.out.println("2 to 3");
			arr1[2].addEdge(arr1[3]);

			System.out.println("3 to 0");
			arr1[3].addEdge(arr1[0]);
			System.out.println("3 to 2");
			arr1[3].addEdge(arr1[2]);

			System.out.println("1 to 2");
			arr1[1].addEdge(arr1[2]);
			System.out.println("1 to 3");
			arr1[1].addEdge(arr1[3]);
			System.out.println("2 to 1");
			arr1[2].addEdge(arr1[1]);
			System.out.println("3 to 1");
			arr1[3].addEdge(arr1[1]);

			System.out.println("Adding Nodes for Graph2");
			for (int i = 0; i < arr2.length; i++) {
				arr2[i] = g2.addNode();
			}

			System.out.println("List of Nodes for Graph2");
			for(Node nd : g2.getNodes()){
				System.out.println("Node " + nd.getId());
			}

			System.out.println("Adding Edges for Graph 2");
			System.out.println("0 to 1");
			arr2[0].addEdge(arr2[1]);
			System.out.println("0 to 2");
			arr2[0].addEdge(arr2[2]);
			System.out.println("0 to 3");
			arr2[0].addEdge(arr2[3]);

			System.out.println("1 to 0");
			arr2[1].addEdge(arr2[0]);

			System.out.println("2 to 0");
			arr2[2].addEdge(arr2[0]);
			System.out.println("2 to 3");
			arr2[2].addEdge(arr2[3]);

			System.out.println("3 to 0");
			arr2[3].addEdge(arr2[0]);
			System.out.println("3 to 2");
			arr2[3].addEdge(arr2[2]);

			System.out.println("Adding Nodes for Graph3");
			for (int i = 0; i < arr3.length; i++) {
				arr3[i] = g3.addNode();
			}

			System.out.println("List of Nodes for Graph3");
			for(Node nd : g2.getNodes()){
				System.out.println("Node " + nd.getId());
			}

			System.out.println("Adding Edges for Graph3");

			//cc graph2
			System.out.println("0 to 1");
			arr3[0].addEdge(arr3[1]);
			System.out.println("1 to 2");
			arr3[1].addEdge(arr3[2]);
			System.out.println("1 to 4");
			arr3[1].addEdge(arr3[4]);
			System.out.println("1 to 5");
			arr3[1].addEdge(arr3[5]);

			System.out.println("2 to 1");
			arr3[2].addEdge(arr3[1]);
			System.out.println("2 to 3");
			arr3[2].addEdge(arr3[3]);

			System.out.println("3 to 2");
			arr3[3].addEdge(arr3[2]);
			System.out.println("3 to 4");
			arr3[3].addEdge(arr3[4]);
			System.out.println("3 to 6");
			arr3[3].addEdge(arr3[6]);

			System.out.println("4 to 3");
			arr3[4].addEdge(arr3[3]);
			System.out.println("4 to 6");
			arr3[4].addEdge(arr3[6]);
			System.out.println("4 to 1");
			arr3[4].addEdge(arr3[1]);
			System.out.println("4 to 5");
			arr3[4].addEdge(arr3[5]);

			System.out.println("5 to 1");
			arr3[5].addEdge(arr3[1]);
			System.out.println("5 to 4");
			arr3[5].addEdge(arr3[4]);
			System.out.println("5 to 6");
			arr3[5].addEdge(arr3[6]);

			System.out.println("6 to 5");
			arr3[6].addEdge(arr3[5]);
			System.out.println("6 to 4");
			arr3[6].addEdge(arr3[4]);
			System.out.println("6 to 3");
			arr3[6].addEdge(arr3[3]);

			ClusteringCoefficient cc = new ClusteringCoefficient();
			double ccg1 = 0;
			ccg1 = cc.run(g1, false);
			assert ccg1 == 1.0;
			System.out.println("cc for graph1 = " + ccg1);

			double ccg2 = 0;
			ccg2 = cc.run(g2, false);
			assert ccg2 == 0.5833333333333333;
			System.out.println("cc for graph2 = " + ccg2);

			double ccg3 = 0;
			ccg3 = cc.run(g3, false);
			assert ccg3 == 0.3571428571428571;
			System.out.println("cc for graph3 = " + ccg3);
		}
		finally {
			if(gm != null)
				gm.shutdown();
		}
	}
}

package graphdb.algo;

import graphdb.graph.BufferConfiguration;
import graphdb.graph.Edge;
import graphdb.graph.EdgeDirection;
import graphdb.graph.Graph;
import graphdb.graph.GraphManager;
import graphdb.graph.GraphQueryAlgorithm;
import graphdb.graph.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KHopRandomWalk<T> implements GraphQueryAlgorithm{
	private static Logger log = Logger.getLogger(KHopRandomWalk.class.getName());

	public Node run(Node root, int maxHops, long targetId){
		Random rand = new Random((long)43);
		Node nextElement  = root;

		log.trace("Random walk searching at hop count: 0");
		//comparison for search element for the root node itself
		if(Long.valueOf(root.getId()).equals(Long.valueOf(targetId))){
			//result is found return it
			return root;
		}
		log.trace("Random walk looking for:"+ targetId + " found:" + root.getId());

		for (int hopCount = 1; hopCount <= maxHops; hopCount++) {
			List<Node> neighbors = new ArrayList<Node>();

			log.trace("Random walk searching at hop count: " + hopCount);

			log.trace("Random walk searching at neighbors of node:" + nextElement);
			for(Edge e : nextElement.getEdges(EdgeDirection.OUT)){
				Node u = e.getDestination();
				neighbors.add(u); // add neighbors to a list

				//comparison for search element on neighbors
				if(Long.valueOf(u.getId()).equals(Long.valueOf(targetId))){
					//result is found return it
					return u;
				}
				log.trace("Random walk looking for:"+ targetId + " found:" + u.getId());
			}


			log.trace("Random walk next element selection set:" + neighbors);

			//if no out edges
			if(neighbors.isEmpty()){
				log.trace("Random walk cannot proceed since there is no out edges.");
				return null;
			}
			else {
				//select a neighbor of n randomly
				nextElement = neighbors.get(rand.nextInt(neighbors.size()));
				log.trace("Random walk selected next neighbor :" + nextElement);
			}
		}

		return null;
	}

	public Node run(Node root, int maxHops, String searchPropKey, T searchProp){
		Random rand = new Random();
		Node nextElement  = root;

		log.trace("Random walk searching at hop count: 0");
		//comparison for search element for the root node itself
		if(root.getProperty(searchPropKey) != null){
			if(root.getProperty(searchPropKey).equals(searchProp)){
				//result is found return it
				return root;
			}
			log.trace("Random walk looking for:"+ searchProp + " found:" + root.getProperty(searchPropKey));
		}

		for (int hopCount = 1; hopCount <= maxHops; hopCount++) {
			List<Node> neighbors = new ArrayList<Node>();

			log.trace("Random walk searching at hop count: " + hopCount);

			log.trace("Random walk searching at neighbors of node:" + nextElement);
			for(Edge e : nextElement.getEdges(EdgeDirection.OUT)){
				Node u = e.getDestination();
				neighbors.add(u); // add neighbors to a list

				//comparison for search element on neighbors
				if(u.getProperty(searchPropKey) != null){
					log.trace("Random walk looking for:"+ searchProp + " found:" + u.getProperty(searchPropKey));
					if(u.getProperty(searchPropKey).equals(searchProp)){
						//result is found return it
						return u;
					}
				}
			}


			log.trace("Random walk next element selection set:" + neighbors);

			//if no out edges
			if(neighbors.isEmpty()){
				log.trace("Random walk cannot proceed since there is no out edges.");
				return null;
			}
			else {
				//select a neighbor of n randomly
				nextElement = neighbors.get(rand.nextInt(neighbors.size()));
				log.trace("Random walk selected next neighbor :" + nextElement);
			}
		}

		return null;
	}
	public static void main(String[] args){
		Logger.getRootLogger().setLevel(Level.TRACE);

		GraphManager gm = null;
		try{
			Node[] arr = new Node[10];

			gm = GraphManager.getInstance();
			BufferConfiguration bc = new BufferConfiguration(3,100);
			gm.setBufferConfiguration(bc);

			gm.deleteEverything();

			Graph g1 = gm.createGraph();

			System.out.println("Adding Nodes for Graph1");
			for (int i = 0; i < arr.length; i++) {
				arr[i] = g1.addNode();
				//				arr[i].setProperty("id", i);
			}

			System.out.println("List of Nodes for Graph1");
			for(Node nd : g1.getNodes()){
				System.out.println("Node " + nd.getId());
			}

			System.out.println("Adding Edges for Graph1");
			System.out.println("0 to 1");
			arr[0].addEdge(arr[1]);
			System.out.println("0 to 2");
			arr[0].addEdge(arr[2]);
			System.out.println("1 to 5");
			arr[1].addEdge(arr[5]);
			System.out.println("2 to 4");
			arr[2].addEdge(arr[4]);
			System.out.println("2 to 5");
			arr[2].addEdge(arr[5]);
			System.out.println("3 to 2");
			arr[3].addEdge(arr[2]);
			System.out.println("4 to 3");
			arr[4].addEdge(arr[3]);
			System.out.println("4 to 7");
			arr[4].addEdge(arr[7]);
			System.out.println("5 to 6");
			arr[5].addEdge(arr[6]);
			System.out.println("5 to 7");
			arr[5].addEdge(arr[7]);
			System.out.println("6 to 1");
			arr[6].addEdge(arr[1]);
			System.out.println("7 to 4");
			arr[7].addEdge(arr[4]);
			System.out.println("8 to 7");
			arr[8].addEdge(arr[7]);

			System.out.println("7 to 9");
			arr[7].addEdge(arr[9]);


			KHopRandomWalk<Integer> rw = new KHopRandomWalk<Integer>();

			Node n = null;

			//			n = rw.run(arr[0], 3, "id", 7);
			//			System.out.println("Found node is " + n);
			//
			//			n = rw.run(arr[0], 2, "id", 7);
			//			System.out.println("Found node is " + n);

			n = rw.run(arr[0], 3, (long) 3);
			System.out.println("Found node is " + n);

			n = rw.run(arr[0], 6, (long) 3);
			System.out.println("Found node is " + n);

			//			n = rw.run(arr[9], 10, "id", 100);

		}
		finally {
			if(gm != null)
				gm.shutdown();
		}
	}
}

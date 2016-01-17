package graphdb.algo;

import graphdb.graph.BufferConfiguration;
import graphdb.graph.Edge;
import graphdb.graph.EdgeDirection;
import graphdb.graph.Graph;
import graphdb.graph.GraphManager;
import graphdb.graph.GraphQueryAlgorithm;
import graphdb.graph.Node;
import graphdb.util.Pair;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KHopBFS<T> implements GraphQueryAlgorithm{
	private static Logger log = Logger.getLogger(KHopBFS.class.getName());

	public Node run(Node root, int maxHops, long targetId){
		Queue<Pair<Node,Integer>> q = new LinkedList<Pair<Node,Integer>>();
		HashSet<Long> visited = new HashSet<Long>();
		int hopCount = 0;	// the level, hop count

		q.add(new Pair<Node, Integer>(root,0));

		while(!q.isEmpty() && hopCount <= maxHops){
			Pair<Node, Integer> p = q.peek();
			Node n = p.getLeft();
			hopCount = p.getRight();

			log.trace("BFS queue for iteration:" + q);
			log.trace("BFS processing node:"+n + " hopCount:" + hopCount);

			//comparison for search element
			if(Long.valueOf(n.getId()).equals(Long.valueOf(targetId))){
				//result is found return it
				return n;
			}

			log.trace("BFS Looking for:"+ targetId + " found:" + n);

			// for each neighbor of n
			for(Edge e : n.getEdges(EdgeDirection.OUT)){
				//if the destination is already visited don't bring it again
				Long u = e.getDestinationId();

				if(!visited.contains(u)){
					visited.add(u);
					q.add(new Pair<Node,Integer>(e.getDestination(), hopCount+1));
				}
			}

			//remove n from queue
			q.poll();
		}

		return null;
	}
	public Node run(Node root, int maxHops, Node target){
		Queue<Pair<Node,Integer>> q = new LinkedList<Pair<Node,Integer>>();
		HashSet<Long> visited = new HashSet<Long>();
		int hopCount = 0;	// the level, hop count

		q.add(new Pair<Node, Integer>(root,0));

		while(!q.isEmpty() && hopCount <= maxHops){
			Pair<Node, Integer> p = q.peek();
			Node n = p.getLeft();
			hopCount = p.getRight();

			log.trace("BFS queue for iteration:" + q);
			log.trace("BFS processing node:"+n + " hopCount:" + hopCount);
			//comparison for search element
			if(n.equals(target)){
				//result is found return it
				return n;
			}

			log.trace("BFS Looking for:"+ target + " found:" + n);

			// for each neighbor of n
			for(Edge e : n.getEdges(EdgeDirection.OUT)){
				//if the destination is already visited don't bring it again
				Long u = e.getDestinationId();

				if(!visited.contains(u)){
					visited.add(u);
					q.add(new Pair<Node,Integer>(e.getDestination(), hopCount+1));
				}
			}

			//remove n from queue
			q.poll();
		}

		return null;
	}
	public Node run(Node root, int maxHops, String searchPropKey, T searchProp){
		Queue<Pair<Node,Integer>> q = new LinkedList<Pair<Node,Integer>>();
		HashSet<Long> visited = new HashSet<Long>();
		int hopCount = 0;	// the level, hop count

		q.add(new Pair<Node, Integer>(root,0));

		while(!q.isEmpty() && hopCount <= maxHops){
			Pair<Node, Integer> p = q.peek();
			Node n = p.getLeft();
			hopCount = p.getRight();

			log.trace("BFS queue for iteration:" + q);
			log.trace("BFS processing node:"+n + " hopCount:" + hopCount);
			//comparison for search element
			if(n.getProperty(searchPropKey) != null){
				//if the destination is already visited don't bring it again
				if(n.getProperty(searchPropKey).equals(searchProp)){
					//result is found return it
					return n;
				}
				log.trace("BFS Looking for:"+ searchProp + " found:" + n.getProperty(searchPropKey));
			}

			// for each neighbor of n
			for(Edge e : n.getEdges(EdgeDirection.OUT)){
				Long u = e.getDestinationId();

				if(!visited.contains(u)){
					visited.add(u);
					q.add(new Pair<Node,Integer>(e.getDestination(), hopCount+1));
				}
			}

			//remove n from queue
			q.poll();
		}

		return null;
	}
	public static void main(String[] args){
		Logger.getRootLogger().setLevel(Level.DEBUG);

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


			KHopBFS<Integer> bfs = new KHopBFS<Integer>();
			//			KHopRandomWalk<Integer> rw = new KHopRandomWalk<Integer>();

			Node n = null;

			//			n = bfs.run(arr[0], 3, "id", 7);
			//			System.out.println("Found node is " + n);
			//
			//			n = bfs.run(arr[0], 2, "id", 7);
			//			System.out.println("Found node is " + n);
			//			n = rw.run(arr[9], 10, "id", 100);

			n = bfs.run(arr[0], 3, 7);
			System.out.println("Found node is " + n);

			n = bfs.run(arr[0], 2, 7);
			System.out.println("Found node is " + n);

		}
		finally {
			if(gm != null)
				gm.shutdown();
		}
	}
}

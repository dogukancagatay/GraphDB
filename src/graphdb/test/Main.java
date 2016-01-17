package graphdb.test;

import graphdb.graph.BufferConfiguration;
import graphdb.graph.Edge;
import graphdb.graph.Graph;
import graphdb.graph.GraphManager;
import graphdb.graph.Node;
import graphdb.policy.SubgraphBFSSplit;
import graphdb.policy.SubgraphRandomSelect;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;



public class Main {

	public static void main(String[] args) {
		Logger.getRootLogger().setLevel(Level.DEBUG);

		GraphManager gm = null;
		try{
			Node[] arr = new Node[10];

			gm = GraphManager.getInstance();
			gm.setSubgraphSplitPolicy(new SubgraphBFSSplit());
			//			gm.setSubgraphSplitPolicy(new SubgraphAffinitySplit());
			gm.setSubgraphSelectPolicy(new SubgraphRandomSelect());

			BufferConfiguration buffconf = new BufferConfiguration(6,150);
			gm.setBufferConfiguration(buffconf);

			Graph g = gm.createGraph("graph1");

			System.out.println("Graph id = " + g.getId());
			System.out.println("Graph name = " + g.getName());

			System.out.println("adding nodes");
			for (int i = 0; i < arr.length; i++) {
				System.out.println("Node added." + i );
				arr[i] = g.addNode();
			}

			System.out.println("nodes");
			for(Node nd : g.getNodes()){
				System.out.println("Node " + nd.getId());
			}

			g.printSubgraphs("After all nodes are imported.");

			Edge e = null;
			System.out.println("adding edges");

			System.out.println("1) 0 to 1");
			e = arr[0].addEdge(arr[1]);
			g.printSubgraphs("After edge added.");

			e.setProperty("id", arr[0] + " to " + arr[1]);
			g.printSubgraphs("After edge property added.");

			System.out.println("2) 0 to 2");
			e = arr[0].addEdge(arr[2]);
			g.printSubgraphs("After edge added.");

			e.setProperty("id", arr[0] + " to " + arr[2]);
			g.printSubgraphs("After edge property added.");

			System.out.println("3) 1 to 5");
			e = arr[1].addEdge(arr[5]);
			g.printSubgraphs("After edge added.");

			e.setProperty("id", arr[1] + " to " + arr[5]);
			g.printSubgraphs("After edge property added.");

			System.out.println("4) 2 to 4");
			e = arr[2].addEdge(arr[4]);
			g.printSubgraphs("After edge added.");

			e.setProperty("id", arr[2] + " to " + arr[4]);
			g.printSubgraphs("After edge property added.");

			System.out.println("5) 2 to 5");
			e = arr[2].addEdge(arr[5]);
			g.printSubgraphs("After edge added.");

			e.setProperty("id", arr[2] + " to " + arr[5]);
			g.printSubgraphs("After edge property added.");

			System.out.println("6) 3 to 2");
			e = arr[3].addEdge(arr[2]);
			g.printSubgraphs("After edge added.");

			e.setProperty("id", arr[3] + " to " + arr[2]);
			g.printSubgraphs("After edge property added.");

			System.out.println("7) 4 to 3");
			e = arr[4].addEdge(arr[3]);
			g.printSubgraphs("After edge added.");

			e.setProperty("id", arr[4] + " to " + arr[3]);
			g.printSubgraphs("After edge property added.");

			System.out.println("8) 4 to 7");
			e = arr[4].addEdge(arr[7]);
			g.printSubgraphs("After edge added.");

			e.setProperty("id", arr[4] + " to " + arr[7]);
			g.printSubgraphs("After edge property added.");

			System.out.println("9) 5 to 6");
			e = arr[5].addEdge(arr[6]);
			g.printSubgraphs("After edge added.");

			e.setProperty("id", arr[5] + " to " + arr[6]);
			g.printSubgraphs("After edge property added.");

			System.out.println("10) 5 to 7");
			e = arr[5].addEdge(arr[7]);
			g.printSubgraphs("After edge added.");

			e.setProperty("id", arr[5] + " to " + arr[7]);
			g.printSubgraphs("After edge property added.");

			System.out.println("11) 6 to 1");
			e = arr[6].addEdge(arr[1]);
			g.printSubgraphs("After edge added.");

			e.setProperty("id", arr[6] + " to " + arr[1]);
			g.printSubgraphs("After edge property added.");

			System.out.println("12) 7 to 4");
			e = arr[7].addEdge(arr[4]);
			g.printSubgraphs("After edge added.");

			e.setProperty("id", arr[7] + " to " + arr[4]);

			System.out.println("13) 8 to 7");
			e = arr[8].addEdge(arr[7]);
			g.printSubgraphs("After edge added.");

			e.setProperty("id", arr[8] + " to " + arr[7]);
			g.printSubgraphs("After edge property added.");

			System.out.println("14) 7 to 9");
			e = arr[7].addEdge(arr[9]);
			g.printSubgraphs("After edge added.");

			e.setProperty("id", arr[7] + " to " + arr[9]);
			g.printSubgraphs("After edge property added.");

			System.out.println("Done.");
		}
		finally {
			if(gm != null)
				gm.shutdown();
		}
	}

}

package graphdb.algo;

import graphdb.graph.Edge;
import graphdb.graph.EdgeDirection;
import graphdb.graph.Graph;
import graphdb.graph.GraphQueryAlgorithm;
import graphdb.graph.Node;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.Logger;

public class Pagerank implements GraphQueryAlgorithm{
	private static Logger log = Logger.getLogger(Pagerank.class.getName());

	double dampingFactor;
	HashSet<Long> leafNodeSet;
	HashMap<Long, Double> currentScore;
	HashMap<Long, Double> outputScore;
	int numNodes;
	boolean outputReversed;
	double stepDiff;
	Graph g;


	public Pagerank(double dampingFactor, Graph g){
		this.g = g;
		numNodes = (int) g.getNumNodes();

		this.dampingFactor = dampingFactor;
		this.leafNodeSet = new HashSet<Long>();

		this.currentScore = new HashMap<Long,Double>(numNodes);
		this.outputScore = new HashMap<Long,Double>(numNodes);


		this.outputReversed = false;
		this.stepDiff = Double.MIN_VALUE;

		//		disappearingPotential = 0.0;

		log.info("Starting initializing rankings...");
		initializeRankings();
		log.info("Finishing initializing rankings...");
	}

	private void swapOutputAndCurrent(){
		HashMap<Long,Double> tmp = outputScore;
		outputScore = currentScore;
		currentScore = tmp;

		outputReversed = !outputReversed;
	}

	private void initializeRankings() {
		double pr = 1.0 / numNodes; //initial pr value for each node

		//set scores to 1/n
		for(Node n : g.getNodes()){
			if(!n.hasEdge(EdgeDirection.OUT) && n.hasEdge(EdgeDirection.IN)){ //if no out edge but in edge exists
				leafNodeSet.add(n.getId());
			}

			if(n.hasEdge(EdgeDirection.IN)){
				currentScore.put(n.getId(), pr);
				outputScore.put(n.getId(), pr);
			} else {
				currentScore.put(n.getId(), 0.0);
				outputScore.put(n.getId(), 0.0);
			}
		}
	}

	private double update(Node n){
		double nSum = 0.0;

		//		if(leafNodeSet.contains(i)){
		//			//collect disappearing potential from leaf nodes.
		//			disappearingPotential += currentScore.get(i);
		//		}

		for(Edge e : n.getEdges(EdgeDirection.IN)){
			Node startnode = e.getSource();

			if(!startnode.equals(n) && n.hasEdge(EdgeDirection.OUT)){
				nSum += (currentScore.get(startnode.getId()).doubleValue() / (double)n.getNumEdges(EdgeDirection.OUT));
			}
		}

		double curPRSum = nSum * (dampingFactor) + (1 - dampingFactor) / numNodes;
		outputScore.put(n.getId(), curPRSum);

		return Math.abs(currentScore.get(n.getId()) - curPRSum);
	}

	private void step(){
		swapOutputAndCurrent();
		for(Node n : g.getNodes()){
			double diff = update(n);
			updateMaxDiff(diff);
		}

		//		afterStep();
	}

	private void updateMaxDiff(double diff){
		stepDiff += diff;
	}

	//	private void afterStep(){
	//		if(disappearingPotential > 0){
	//			for (int n = 0; n < adjlist.length; n++) {
	//				currentScore.put(n, currentScore.get(n) + ((1 - dampingFactor) * disappearingPotential / numNodes));
	//			}
	//		}
	//		disappearingPotential = 0.0;
	//	}

	public void evaluate() {
		int iterations = 0;
		int maximumIterations = 10000;
		double desiredPrecision = 0.00001;
		double oldStepDiff = 0.0;

		while(iterations++ < maximumIterations) {
			log.info("Iteration : "+ iterations);
			step();
			log.info("MaxDiff : " + stepDiff);
			if(Math.abs(oldStepDiff - stepDiff) < desiredPrecision){
				break;
			}
			oldStepDiff = stepDiff;
			stepDiff = 0.0;
		}
	}

	//	private double getEdgeWeight(Relationship r){
	//		return 1.0;
	//	}
	//

	public void printRankings(){
		if(outputReversed){
			for (Iterator<Long> it = currentScore.keySet().iterator(); it.hasNext();) {
				long nodeid = it.next();

				System.out.println(nodeid + "," + currentScore.get(nodeid));
			}
		}
		else {
			for (Iterator<Long> it = outputScore.keySet().iterator(); it.hasNext();) {
				long nodeid = it.next();

				System.out.println(nodeid + "," + outputScore.get(nodeid));
			}
		}
	}

	public void writeRankingsTofile(String filename){
		FileWriter fw = null;
		try {
			fw = new FileWriter(new File(filename));
		} catch (IOException e) {
			e.printStackTrace();
		}
		PrintWriter outFile = new PrintWriter(fw);

		if(outputReversed){
			for (Iterator<Long> it = currentScore.keySet().iterator(); it.hasNext();) {
				long nodeid = it.next();

				outFile.println(nodeid + "," + currentScore.get(nodeid));
			}
		}
		else {
			for (Iterator<Long> it = outputScore.keySet().iterator(); it.hasNext();) {
				long nodeid = it.next();

				outFile.println(nodeid + "," + outputScore.get(nodeid));
			}
		}
		outFile.close();
	}
}

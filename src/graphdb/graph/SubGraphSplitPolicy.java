package graphdb.graph;

import java.util.List;

public interface SubGraphSplitPolicy {
	public List<SubGraph> split(SuperGraph superg, SubGraph oldsg);
	public void addEdgeAction(Edge e);
}
package graphdb.graph;

public interface SubGraphSelectPolicy {
	public SubGraph select(Node n, SuperGraph g);
}

package graphdb.graph;

public class Edge {
//	private long id;
	private long srcId;
	private long destId;
	
	private EdgeDirection direction;
	private SuperGraph g;
	
	public Edge(Node source, Node destination){
		this.g = (SuperGraph) source.getGraph();
		this.srcId = source.getId();
		this.destId = destination.getId();

		this.direction = EdgeDirection.OUT;
	}
	
//	public Edge(Graph g, long id, Node source, Node destination, EdgeDirection direction){
//		this.g = (SuperGraph) g;
//		this.srcId = source.getId();
//		this.destId = destination.getId();
//		this.direction = direction;
//	}
	public Edge(Graph g, long id, Node source, Node destination){
		this.g = (SuperGraph) g;
		this.srcId = source.getId();
		this.destId = destination.getId();

		this.direction = EdgeDirection.OUT;
	}
	
	public Node getSource() {
		return g.getNode(srcId);
	}
	
	public long getSourceId(){
		return srcId;
	}
	
	public Node getDestination() {
		return g.getNode(destId);
	}
	
	public long getDestinationId(){
		return destId;
	}

	public EdgeDirection getDirection() {
		return direction;
	}

	public void setDirection(EdgeDirection direction) {
		this.direction = direction;
	}
	
	public Edge setProperty(String key, Object value){
		return g.setProperty(this, key, value);
	}

	public Object getProperty(String key){
		return g.getProperty(this, key);
	}
	
	public boolean hasProperty() {
		return g.hasProperty(this);
	}
	
	public Graph getGraph(){
		return (Graph) g;
	}
	
	protected boolean isCross(){
		Node src = getSource();
		Node dest = getDestination();
		
		SubGraph srcsg = g.getSubgraphOfNode(src);
		SubGraph destsg = g.getSubgraphOfNode(dest);
		
		return srcsg.equals(destsg);
	}
	
	@Override
	public String toString() {
		return srcId + (g.onDisk(srcId) ? "(disk)": "(mem)") + " -> " + destId + (g.onDisk(destId) ? "(disk)": "(mem)") + (g.isCross(srcId, destId) ? " (cross)":"");
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Edge){
			Edge e = (Edge) obj;
			return e.getSourceId() == srcId && e.getDestinationId() == destId && g.getId() == e.getGraph().getId();
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		int result = (int) (g.getId() ^ (g.getId() >>> 32));
        result = 31 * result + (int) (srcId ^ (srcId >>> 32));
        result = 31 * result + (int) (destId ^ (destId >>> 32));
        return result;
//		return new Integer(g.getId()).hashCode() ^ new Long(srcId).hashCode() ^ new Long(destId).hashCode();
	}

}

package graphdb.graph;



public class Node {
	private long id;
	private SuperGraph g;
	private boolean dummy;
	
	protected Node (SuperGraph g,long id){
		this.g = g;
		this.id = id;
		dummy = false;
	}
	
	protected Node (SuperGraph g,long id, boolean isDummy){
		this.g = g;
		this.id = id;
		this.dummy = isDummy;
	}
	
	public long getId(){
		return id;
	}
	
	public Iterable<Edge> getEdges(){
		ifDummy();
		return g.getEdges(this);
	}
	
	public Iterable<Edge> getEdges(EdgeDirection dir){
		ifDummy();
		return g.getEdges(this, dir);
	}
	
	public Object getProperty(String key){
		ifDummy();
		return g.getProperty(this, key);
	}
	
	public Node setProperty(String key, Object value){
		ifDummy();
		return g.setProperty(this, key, value);
	}
	
	public Edge addEdge(Node dest){
		return g.addEdge(this, dest);
	}
	
	public boolean hasProperty(){
		ifDummy();
		return g.nodeHasProperty(id);
	}
	
	protected boolean isDummy(){
		return dummy;
	}
	
	protected void setDummy(boolean status){
		this.dummy = status;
	}
	
	protected void ifDummy(){
		//issue access to the page in the buffer to prevent the node to be written to the 
		if(dummy){
			g.getNode(id); //bring the node's subgraph from db
			g.swapDummyNode(this);
			dummy = false;
		}
	}
	
	public Graph getGraph(){
		return (Graph) g;
	}
	
	public boolean hasEdge(){
		if(hasEdge(EdgeDirection.OUT))
			return true;
		if(hasEdge(EdgeDirection.IN))
			return true;
		return false;
	}

	@SuppressWarnings("unused") 
	public boolean hasEdge(EdgeDirection dir){
		for(Edge e : this.getEdges(dir)){
			return true;
		}
		return false;
	}
	

	public int getNumEdges(EdgeDirection dir){
		// TODO test getNumEdges 
		return g.getNumEdges(this,dir);
	}

	public int getNumEdges(){
		// TODO test getNumEdges 
		return g.getNumEdges(this, EdgeDirection.BOTH);
	}

	@Override
	public int hashCode() {
		int result = (int) (g.getId() ^ (g.getId() >>> 32));
        result = 31 * result + (int) (id ^ (id >>> 32));
        return result;
//		return new Integer(g.gid).hashCode() ^ new Long(id).hashCode();
	}
	
	@Override
	public String toString() {
		return Long.toString(id);
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Node){
			Node n = (Node) obj;
			return n.getId() == id && g.getId() == n.getGraph().getId();
		}
		return false;
	}
}

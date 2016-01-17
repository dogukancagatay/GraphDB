package graphdb.graph;

import graphdb.util.DoublyLinkedList;



public class Page {
	private long pid;
	
	private int gid;
	private int sgid;
	
	private DoublyLinkedList<Page> adjacentPages;
	private boolean partitioned;
	private boolean dirty;

	private byte[] data;

	public Page(long pageid, int gid, int sgid, DoublyLinkedList<Page> adjlist, byte[] data) {
		this.pid = pageid;
		this.sgid = sgid;
		this.gid = gid;
		
		this.dirty = false;
		
		if(adjlist == null){ // not partitioned
			adjacentPages = new DoublyLinkedList<Page>();
			partitioned = false;
		}
		else {
			// doubly linked list will be shared between partitioned pages
			partitioned = true;
			adjacentPages = adjlist;
		}
		
		//add itself to its adj list
		adjacentPages.appendElement(this);
		
		//set its data
		this.data = data;
	}

	public long getId() {
		return pid;
	}

	public int getGraphId() {
		return gid;
	}

	public int getSubgraphId() {
		return adjacentPages.getFirstElement().sgid;
	}
	
	public DoublyLinkedList<Page> getAdjacentPagesList(){
		return this.adjacentPages;
	}
	
	public boolean isPartitioned(){
		return partitioned;
	}
	
	public void setPartitioned(boolean flag){
		this.partitioned = flag;
	}
	
	public DoublyLinkedList<Page> setAdjacentPages(DoublyLinkedList<Page> dll){
		adjacentPages = dll;
		return adjacentPages;
	}
	
	public void setData(byte[] data){
		this.data = data;
	}

	public boolean equals(Page p) {
		return this.pid == p.getId() && this.gid == p.getGraphId()
				&& this.sgid == p.getSubgraphId();
	}

	public byte[] getData() {
		return data;
	}

	public void setSubgraph(int sgid) {
		this.sgid = sgid;
	}
	
	public void setDirty(boolean status){
		if(this.equals(adjacentPages.getFirstElement())){
			dirty = status;
		}
		else{
			adjacentPages.getFirstElement().setDirty(status);
		}
	}
	
	public boolean isDirty() {
		if(this.equals(adjacentPages.getFirstElement())){
			return dirty;
		}
		else {
			return adjacentPages.getFirstElement().isDirty();
		}
	}
	
	@Override
	public boolean equals(Object obj) {
		Page other = null;
		if(obj instanceof Page){
			other = (Page) obj;
			return other.getId() == getId();
		}
		return false;
	}
	
	@Override
	public String toString(){
		return new String("Page" + pid+ "(" + (partitioned ? "P," : "NP,") + "SubGraph" + sgid + ",Size" + (data == null ? 0 : data.length) + ")");
	}
	
}

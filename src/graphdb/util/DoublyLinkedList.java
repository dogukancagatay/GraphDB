package graphdb.util;

import java.util.Iterator;

/**
 * A doubly linked list implementation
 * 
 * @param <T> The type of the data items stored in the list
 */
public class DoublyLinkedList<T> implements Cloneable, Iterable<T>
{
	//this class was not public
	public static class Node<T> {
		public T data;
		public Node<T> next;
		public Node<T> prev;
		Node(T data) {
			this.data = data;
		}
	}
	
	private Node<T> first;
	private Node<T> last;
	private int size;
	
	/**
	 * Get the size of the list
	 * @return the size of the list
	 */
	public int getSize() {
		return size;
	}
	
	/**
	 * Check if the list is empty
	 * @return true if the list is empty, false otherwise
	 */
	public boolean isEmpty() {
		return (first==null);
	}
	
	/**
	 * Get the last element of the list
	 * @return the last element of the list, null if not present
	 */
	public T getLastElement() {
		if (isEmpty())
			return null;
		return last.data;
	}
	
	/**
	 * Get the last node of the list
	 * @return the last node of the list, null if not present
	 */
	public Node<T> getLastNode() {
		return last;
	}
	
	/**
	 * Get the first element of the list
	 * @return the first element of the list, null if not present
	 */
	public T getFirstElement() {
		if (isEmpty())
			return null;
		return first.data;
	}
	
	/**
	 * Get the first node of the list
	 * @return the first node of the list, null if not present
	 */
	public Node<T> getFirstNode() {
		return first;
	}
	
	/** 
	 * Append an element to the list
	 * @param e element to append
	 * @return the appended node
	 */
	public Node<T> appendElement(T e) {
		Node<T> n = new Node<T>(e);
		if (isEmpty()) {
			first = last = n;
		} else {
			last.next = n;
			n.prev = last;
			last = n;
		}
		size++;
		return n;
	}
	
	/**
	 * Prepend an element to the list
	 * @param e element to prepend
	 * @return the appended node
	 */
	public Node<T> prependElement(T e) {
		Node<T> n = new Node<T>(e);
		if (isEmpty()) {
			first = last = n;
		} else {
			first.prev = n;
			n.next = first;
			first = n;
		}
		size++;
		return n;
	}
	
	/**
	 * Remove the first element from the list
	 * @return true if removed, false if the list was empty
	 */
	public boolean removeFirstElement() {
		if (isEmpty())
			return false;
		first = first.next;
		if (first!=null)
			first.prev = null;
		else
			last = null;
		size--;
		return true;
	}
	
	/**
	 * Remove the last element from the list
	 * @return true of removed, false if the list was empty
	 */
	public boolean removeLastElement() {
		if (isEmpty()) 
			return false;
		last = last.prev;
		if (last!=null)
			last.next = null;
		else
			first = null;
		size--;
		return true;
	}
	
	/**
	 * Get the element at the specified index
	 * @param index index of the element
	 * @return The element at the specified index, or null of not present
	 */
	public T getElementAt(int index) {
		Node<T> node = getNodeAt(index);
		if (node==null)
			return null;
		return node.data;
	}
	
	/**
	 * Get the node at the specified index
	 * @param index index of the node
	 * @return The node at the specified index, or null of not present
	 */
	public Node<T> getNodeAt(int index) {
		if (index<0)
			return null;
		Node<T> curr = first;
		for (int i=0; i<index && curr!=null; ++i) 
			curr = curr.next;
		return curr;
	}
	
	/**
	 * Insert an element at the specified index
	 * @param e element to insert
	 * @param index index at which the element should be inserted (in range [0..size()])
	 * @return true if inserted, false if the index was not valid 
	 */
	public boolean insertElementAt(T e, int index) {
		if (index==size) {
			appendElement(e);
		} else {
			Node<T> n = getNodeAt(index);
			if (n==null) 
				return false;
			Node<T> c = new Node<T>(e);
			insertNode(c, n);
		}
		return true;
	}
	
	/**
	 * Insert a new node at the position of another node
	 * @param c The new node to be inserted
	 * @param n The node at the insertion position
	 */
	private void insertNode(Node<T> c, Node<T> n) {
		Node<T> nP = n.prev;
		c.prev = nP;
		if (nP!=null)
			nP.next = c;
		else
			first = c;
		c.next = n;
		n.prev = c;
		size++;
	}
	
	/**
	 * Remove the element at the given index
	 * @param index index of the element to remove ([0..size()-1]) 
	 * @return true if removed, false if the index was invalid
	 */
	public boolean removeElementAt(int index) {
		Node<T> n = getNodeAt(index);
		if (n==null) 
			return false;
		removeNode(n);
		return true;
	}
	
	/**
	 * Remove a given node from the list
	 * @param n the note to remove
	 */
	public void removeNode(Node<T> n) {
		Node<T> nP = n.prev;
		Node<T> nN = n.next;
		if (nP!=null)
			nP.next = nN;
		else
			first = nN;
		if (nN!=null)
			nN.prev = nP;
		else 
			last = nP;
		size--;
	}
	
	/**
	 * Move the given node to the front of the list
	 * @param n the node to move
	 */
	public void moveNodeToFront(Node<T> n) {
		if (size==1 || first==n)
			return;
		removeNode(n);
		insertNode(n, first);
	}
	
	/**
	 * Swap two nodes that are part of this list
	 * @param n1 the first node
	 * @param n2 the second node
	 */
	public void swapNodes(Node<T> n1, Node<T> n2) {
		if(n1==n2) {
			return;
		} else if (n2.next==n1) {
			swapNodes(n2, n1);	
		} else {
			Node<T> n1P = n1.prev;
			Node<T> n1N = n1.next;
			Node<T> n2P = n2.prev;
			Node<T> n2N = n2.next;
			n2.prev = n1P;
			if(n1P!=null)
				n1P.next = n2;
			n1.next = n2N;
			if (n2N!=null)
				n2N.prev = n1;
			if (n1N==n2) {
				n2.next = n1;
				n1.prev = n2;
			} else {
				n2.next = n1N;
				if (n1N!=null)
					n1N.prev = n2;
				n1.prev = n2P;
				if (n2P!=null)
					n2P.next = n1;
			}
		} 
		if (first==n1)
			first=n2;
		else if (first==n2)
			first=n1;
		if (last==n1)
			last=n2;
		else if (last==n2)
			last=n1;
	}
	
	/**
	 * Splice the given list into this one
	 * @param index index to splice at
	 * @param other the list to splice
	 * @return true of successful, false otherwise 
	 */
	boolean splice (int index,  DoublyLinkedList<T> other)
	{
		if (other.isEmpty())
			return true;
		if (isEmpty()) {
			first = other.first;
			last = other.last;
		} else if (index==size) {
			last.next = other.first;
			other.first.prev = last;
			last = other.last;
		} else {
			Node<T> node = getNodeAt(index);
			if(node==first) {
				first.prev = other.last;
				other.last.next = first;
				first = other.first;
			} else if (node!=null) {
				Node<T> pNode = node.prev;
				pNode.next = other.first;
				other.first.prev = pNode;
				other.last.next = node;
				node.prev = other.last;
			} else {
				return false;
			}
		}
		size += other.size;
		other.first = other.last = null;
		other.size = 0;
		return true;
	}
	
	public void clear() {
		first = last = null;
		size = 0;
	}
	
	/**
	 * Clone the list (data items will not be cloned)
	 * @return the cloned list
	 */
	@Override
	public DoublyLinkedList<T> clone() {
		DoublyLinkedList<T> other = new DoublyLinkedList<T>();
		for (T data : this)
			other.appendElement(data);
		return other;
	}
	
	@Override
	public Iterator<T> iterator() {
		return new DoublyLinkedListIterator<T>(this);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		Iterator<T> it = iterator();
		if (it.hasNext()) {
			T data = it.next();
			sb.append(data);
			while (it.hasNext()) {
				sb.append(",");
				sb.append(it.next());
			}
		}
		sb.append("]");
		return sb.toString();
	}
	
	private static class DoublyLinkedListIterator<T> implements Iterator<T> {
		Node<T> curr, prev;
		DoublyLinkedList<T> list;
		public DoublyLinkedListIterator(DoublyLinkedList<T> list) {
			this.list = list;
			curr = list.first;
		}
		
		@Override
		public boolean hasNext() {
			return (curr != null);
		}

		@Override
		public T next() {
			prev = curr;
			curr = curr.next;
			return prev.data;
		}

		@Override
		public void remove() {
			list.removeNode(prev);
		}
	}
	
	public static void main(String[] args) {
		DoublyLinkedList<String> list = new  DoublyLinkedList<String>();
		assert(list.isEmpty());
		
		list.prependElement("a");
		assert(list.getSize()==1);
		assert(!list.isEmpty());
		assert(list.getElementAt(0).equals("a"));
		assert(list.getFirstElement().equals("a"));
		assert(list.getLastElement().equals("a"));
		list.removeLastElement();
		assert(list.getSize()==0);
		assert(list.isEmpty());
		
		list.appendElement("b");
		assert(list.getSize()==1);
		assert(!list.isEmpty());
		assert(list.getElementAt(0).equals("b"));
		assert(list.getFirstElement().equals("b"));
		assert(list.getLastElement().equals("b"));
		list.removeFirstElement();
		assert(list.getSize()==0);
		assert(list.isEmpty());
		
		list.appendElement("c");
		list.prependElement("b");
		list.prependElement("a");
		list.appendElement("d");
		assert(list.getSize()==4);
		assert(!list.isEmpty());
		assert(list.getElementAt(0).equals("a"));
		assert(list.getElementAt(1).equals("b"));
		assert(list.getElementAt(2).equals("c"));
		assert(list.getElementAt(3).equals("d"));
		
		DoublyLinkedList<String> other = (DoublyLinkedList<String>) list.clone();
		Iterator<String> it = list.iterator();
		while (it.hasNext()) {
			it.next();
			it.remove();
		}
		assert(list.isEmpty());
		assert(list.getSize()==0);
		list = other;
		String all = "";
		for (String s : list)
			all += s;
		assert(all.equals("abcd"));
		
		assert(list.getElementAt(-1)==null);
		assert(list.getElementAt(4)==null);
		list.removeLastElement();
		assert(list.getSize()==3);
		assert(!list.isEmpty());
		list.removeFirstElement();
		assert(list.getSize()==2);
		assert(!list.isEmpty());
		list.removeLastElement();
		assert(list.getSize()==1);
		assert(!list.isEmpty());
		list.removeFirstElement();
		assert(list.getSize()==0);
		assert(list.isEmpty());
			
		boolean b = list.insertElementAt("x", -1);
		assert(b==false);
		b = list.insertElementAt("x", 1);
		assert(b==false);
		b = list.insertElementAt("c", 0);
		assert(b==true);
		b = list.insertElementAt("a", 0);
		assert(b==true);
		b = list.insertElementAt("d", 2);
		assert(b==true);
		b = list.insertElementAt("b", 1);
		assert(b==true);
		assert(list.toString().equals("[a,b,c,d]"));
		
		b = list.removeElementAt(-1); 
		assert(b==false);
		b = list.removeElementAt(4); 
		assert(b==false);
		assert(list.getSize()==4);
		assert(!list.isEmpty());
		b = list.removeElementAt(3);
		assert(b==true);
		assert(list.getSize()==3);
		assert(!list.isEmpty());
		b = list.removeElementAt(1);
		assert(b==true);
		assert(list.getSize()==2);
		assert(!list.isEmpty());
		b = list.removeElementAt(0);
		assert(b==true);
		assert(list.getSize()==1);
		assert(!list.isEmpty());
		b = list.removeElementAt(0);
		assert(b==true);
		assert(list.getSize()==0);
		assert(list.isEmpty());
		assert(b==true);
		
		list.clear();
		Node<String> A = list.appendElement("A");
		Node<String> B = list.appendElement("B");
		Node<String> C = list.appendElement("C");
		Node<String> D = list.appendElement("D");
		Node<String> E = list.appendElement("E");
		Node<String> F = list.appendElement("F");
		assert(list.toString().equals("[A,B,C,D,E,F]"));
		list.swapNodes(B, E);
		assert(list.toString().equals("[A,E,C,D,B,F]"));
		list.swapNodes(C, D);
		assert(list.toString().equals("[A,E,D,C,B,F]"));
		list.swapNodes(A, E);
		assert(list.toString().equals("[E,A,D,C,B,F]"));
		list.swapNodes(B, F);
		assert(list.toString().equals("[E,A,D,C,F,B]"));
		list.swapNodes(E, D);
		assert(list.toString().equals("[D,A,E,C,F,B]"));
		list.swapNodes(C, B);
		assert(list.toString().equals("[D,A,E,B,F,C]"));
		list.swapNodes(B, C);
		assert(list.toString().equals("[D,A,E,C,F,B]"));
		list.swapNodes(D, E);
		assert(list.toString().equals("[E,A,D,C,F,B]"));
		list.swapNodes(F, B);
		assert(list.toString().equals("[E,A,D,C,B,F]"));
		list.swapNodes(E, A);
		assert(list.toString().equals("[A,E,D,C,B,F]"));
		list.swapNodes(D, C);
		assert(list.toString().equals("[A,E,C,D,B,F]"));
		list.swapNodes(E, B);
		assert(list.toString().equals("[A,B,C,D,E,F]"));
		assert(list.toString().equals("[A,B,C,D,E,F]"));
		assert(list.getFirstElement().toString()=="A");
		assert(list.getLastElement().toString()=="F");
		
		DoublyLinkedList<String> list2 = new DoublyLinkedList<String>();
		list2.appendElement("X");
		Node<String> Y = list2.appendElement("Y");
		list2.appendElement("Z");
		list.splice(2,  list2);
		assert(list.toString().equals("[A,B,X,Y,Z,C,D,E,F]"));
		assert(list2.toString().equals("[]"));
		Node<String> U = list2.appendElement("U");
		list.splice(9,  list2);
		assert(list.toString().equals("[A,B,X,Y,Z,C,D,E,F,U]"));
		assert(list2.toString().equals("[]"));
		list2.appendElement("W");
		list.splice(0,  list2);
		assert(list.toString().equals("[W,A,B,X,Y,Z,C,D,E,F,U]"));
		assert(list2.toString().equals("[]"));
		
		list.moveNodeToFront(U);
		assert(list.toString().equals("[U,W,A,B,X,Y,Z,C,D,E,F]"));
		list.moveNodeToFront(U);
		assert(list.toString().equals("[U,W,A,B,X,Y,Z,C,D,E,F]"));
		list.moveNodeToFront(Y);
		assert(list.toString().equals("[Y,U,W,A,B,X,Z,C,D,E,F]"));
		
	}
}

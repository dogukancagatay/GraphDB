package graphdb.util;

import java.util.Iterator;
import java.util.LinkedList;

public class CollectionsIterator<T> implements Iterator<T> {
	private LinkedList<Iterable<T>> collList; 
	private Iterator<Iterable<T>> collIt;
	private Iterator<T> curCollIt;
	
	public CollectionsIterator(){
		collList = new LinkedList<Iterable<T>>();
	}
	
	public void addCollection(Iterable<T> x){
		if(x != null){
			collList.add(x);
		}
		collIt = collList.iterator();
		if(collIt.hasNext()){
			curCollIt = collIt.next().iterator();
		}
	}
	
	public void addCollection(final Iterator<T> x) {
		if(x != null){
			collList.add(new Iterable<T>(){

				@Override
				public Iterator<T> iterator() {
					return x;
				}});
		}
		collIt = collList.iterator();
		if(collIt.hasNext()){
			curCollIt = collIt.next().iterator();
		}
	}
	
	@Override
	public boolean hasNext() {
		if(curCollIt.hasNext()){
			return true;
		}
		else {
			if(collIt.hasNext()){
				Iterator<T> tempIt = null;
				while(collIt.hasNext() && !(tempIt = collIt.next().iterator()).hasNext());
			
				if(tempIt.hasNext()){
					curCollIt = tempIt;
					return true;
				}
				else {
					return false;
				}
			}
			else {
				return false;
			}
		}
	}

	@Override
	public T next() {
		return curCollIt.next();
	}

	@Override
	public void remove() {
	}

	
}

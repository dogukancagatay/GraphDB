package graphdb.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * The class Property models a container for holding properties with their property keys.
 * A Property object can hold any object instance identified by its String property key. It
 * uses a HashMap inside for holding the properties.
 * 
 * @author Dogukan Cagatay
 *
 */
public class Property implements Serializable, Cloneable {
	private static final long serialVersionUID = -6099648428488182108L;
	protected transient Map<String, Object> props;
	private transient int propSize;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static final Set<Class> knownImmutables = new HashSet<Class>(Arrays.asList(
			String.class, Byte.class, Short.class, Integer.class, Long.class,
			Float.class, Double.class, Boolean.class, BigInteger.class, BigDecimal.class
			));

	/**
	 * Default constructor for the class.
	 */
	public Property() {
		props = new HashMap<String, Object>();
		propSize = 0;
	}

	/**
	 * Copy constructor for the class. It copies the a Property object into another. The properties you hold
	 * in the original Property object should not be immutable, otherwise you would get NoSuchMethodException
	 * exception. Built-in java immutable objects are handled. If your custom object you need to hold is immutable
	 * you need to implement its copy constructor.
	 * 
	 * @param orig The Property object you need to copy
	 */
	public Property(Property orig) {
		props = new HashMap<String, Object>();
		propSize = orig.propSize;
		//copy each property one by one
		for (Iterator<String> it = orig.props.keySet().iterator(); it.hasNext();) {
			String key = it.next();
			Object origObj = orig.props.get(key);

			//if the object is immutable then do shallow copy
			if(knownImmutables.contains(origObj.getClass())){
				props.put(key, origObj);
			}
			else {
				//in the new property object make a deep copy of all elements in the original property class
				try {
					props.put(key, origObj.getClass().getConstructor(origObj.getClass()).newInstance(origObj.getClass().cast(origObj)));
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				} catch (NoSuchMethodException e) {
					System.err.println("The object you put in the Property object is immutable. " +
							"Default Java immutable objects can be handled but the other objects that can be held " +
							"by Property object should have a valid copy constructor implemented.");
					e.printStackTrace();

				} catch (SecurityException e) {
					e.printStackTrace();
				}
			}
		}

	}

	/**
	 * Brings the specified property with the property key k. If the key doesn't point to a property it returns null.
	 * You may need to cast the returned value to its original class.
	 * 
	 * @param k Property key(String) of the property, held in the Property object.
	 * @return The property with the key k.
	 */
	public Object getProperty(String k) {
		if (props.containsKey(k)) {
			return props.get(k);
		}
		return null;
	}

	public void setProperty(String key, Object value) {
		props.put(key, value);
		propSize = this._getObjectSize();
	}

	public Set<String> propertyKeySet() {
		return props.keySet();
	}

	public Object removeProperty(String key) {
		if(props.containsKey(key)){
			Object removed = props.get(key);
			props.remove(key);

			propSize = this._getObjectSize();

			return removed;
		}

		return null;
	}

	public boolean isEmpty(){
		if(props.size() == 0){
			return true;
		}

		return false;
	}

	public long size() {
		return props.size();
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();
		out.writeInt(props.size());
		out.writeInt(propSize);

		for (Iterator<String> keys = props.keySet().iterator(); keys
				.hasNext();) {
			String key = (String) keys.next();
			out.writeUTF(key);
			out.writeObject(props.get(key));
		}
	}

	private void readObject(ObjectInputStream in) throws IOException,
	ClassNotFoundException {
		in.defaultReadObject();

		int propNum = in.readInt();
		propSize = in.readInt();

		props = new HashMap<String, Object>(propNum);
		for (int i = 0; i < propNum; i++) {
			String key = in.readUTF();
			Object value = in.readObject();
			props.put(key, value);
		}

		//        propSize = _getObjectSize();
	}

	@Override
	public Property clone() {
		return new Property(this);
	}

	@Override
	public boolean equals(Object obj) {
		if(obj == null)
			return false;

		if(this == obj)
			return true;

		if(!(obj instanceof Property))
			return false;

		final Property other = (Property) obj;
		return this.props.equals(other.props);
	}

	@Override
	public int hashCode() {
		return props.hashCode();
	}

	public String toString(){
		String res = "";
		for (Iterator<String> keys = props.keySet().iterator(); keys
				.hasNext();) {
			String key = (String) keys.next();
			res += "{" + key + " : " + props.get(key) + "}, ";
		}
		return res;
	}
	public int getObjectSize(){
		return propSize;
	}

	private int _getObjectSize() {
		//return writePropertyObject(this).length + 4 + 4; // plus 4 is the integer length of the property object
		return writePropertyObject(this).length;
	}

	public static Property readPropertyObject(byte[] propData) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bais = new ByteArrayInputStream(propData);
		ObjectInputStream ois  = new ObjectInputStream(bais);

		Property prop = (Property) ois.readObject();

		ois.close();
		return prop;
	}

	public static byte[] writePropertyObject(Property prop){
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		try {
			ObjectOutputStream oos = new ObjectOutputStream(baos);

			oos.writeObject(prop);
			oos.close();
			baos.close();

		} catch (Exception e) {
			System.out.println(e);
		}

		return baos.toByteArray();
	}
}

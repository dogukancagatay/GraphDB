package graphdb.connector;

//leveldb native java lib
import static org.iq80.leveldb.impl.Iq80DBFactory.factory;
import graphdb.graph.DatabaseConnector;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
//leveldb-jni
//import static org.fusesource.leveldbjni.JniDBFactory.*;
public class LevelDBConnectorJava implements DatabaseConnector {
	private final byte[] SYSPROPID = Ints.toByteArray(0);
	private final byte[] GLIST_START_ROW = Ints.toByteArray(1);
	
	private String dbDir;
	private final String GRAPH_TABLE = "graph_table";
	private final String NODE_TABLE = "node_table";
	private final String BLOCK_TABLE = "block_table";
	
	private DB graph_tb = null;
	private DB node_tb = null;
	private DB block_tb = null;
	
	private static Logger log = Logger.getLogger(LevelDBConnectorJava.class.getName());
	
	private Options options;
	
	
	public LevelDBConnectorJava(String dbDir) {
		//create options
		options = new Options();
		options.compressionType(CompressionType.NONE);
//		options.createIfMissing(true);
		
		this.dbDir = dbDir;
		
		//open all tables 
		openDb();
	}
	
	private class LevelDBResultIteratorWrapper implements Iterator<Long> {
		private DBIterator it;
		int graphid;
		
		public LevelDBResultIteratorWrapper(DBIterator it, int graphid) {
			this.graphid = graphid;
			this.it = it;
		}
		
		//as long as the graph is the same continue iterating
		@Override
		public boolean hasNext() {
			if(it.hasNext() && (Ints.fromByteArray(splitBytes(it.peekNext().getKey(),Ints.BYTES,0)) == graphid)){
				return true;
			}
			return false;
		}

		@Override
		public Long next() {
			return Longs.fromByteArray(splitBytes(it.next().getKey(), Longs.BYTES, Ints.BYTES));
		}

		@Override
		public void remove() {
			it.remove();
		}
	}
	
	@Override
	public void initGraphDb(byte[] sysprop) {
		//delete all db table directories
		deleteAllData();
		
		//initialize graph, node and block tables
		openDb();
		
		log.debug("Disk access write : initGraphDb");
		//put system properties into graph table
		graph_tb.put(SYSPROPID, sysprop);
		
	}

	@Override
	public byte[] getSystemProperties() {
		//get data from db
		byte[] rowkey = SYSPROPID;
		byte[] res = graph_tb.get(rowkey);
		
		log.debug("Disk access read : getSystemProperties");
		return res;
	}

	@Override
	public HashMap<Integer, byte[]> getGraphList() {
		HashMap<Integer, byte[]> glist = new HashMap<Integer,byte[]>();
		
		log.debug("Disk access read : getGraphList");
		DBIterator it = graph_tb.iterator();
		
		for(it.seek(GLIST_START_ROW); it.hasNext(); it.next()) {
			glist.put(Ints.fromByteArray(it.peekNext().getKey()), it.peekNext().getValue());
		}
		
		return glist;
	}

	@Deprecated
	@Override
	public List<Long> getNodeList(int graphid) {
		List<Long> nlist = new LinkedList<Long>();
		
		DBIterator it = node_tb.iterator();
		//start key is graph id (int) + node id (long)
		byte[] startRowKey = Bytes.concat(Ints.toByteArray(graphid),Longs.toByteArray(0));
		
		//as long as the graph id part doesn't change go on looping
		for(it.seek(startRowKey); it.hasNext(); it.next()){
			byte[] rowkey = it.peekNext().getKey();
			if(Ints.fromByteArray(splitBytes(rowkey,Ints.BYTES,0)) != graphid){
				break;
			}
			nlist.add(Longs.fromByteArray(splitBytes(rowkey, Longs.BYTES, Ints.BYTES)));
		}
		
		return nlist;
	}

	@Override
	public byte[] getBlockList(long nodeid, int graphid) {
		byte[] blist = null;
		
		byte[] rowkey = Bytes.concat(Ints.toByteArray(graphid),Longs.toByteArray(nodeid));
		
		log.debug("Disk access read : getBlockList");
		blist = node_tb.get(rowkey);
		
		return blist;
	}

	@Override
	public byte[] getBlock(long blockid) {
		byte[] block = null;
		
		log.debug("Disk access read : getBlock");
		byte[] rowkey = Longs.toByteArray(blockid);
		block = block_tb.get(rowkey);
		
		return block;
	}

	@Override
	public void writeGraphList(HashMap<Integer, byte[]> glist) {
		WriteBatch batch = graph_tb.createWriteBatch();
		
		log.debug("Disk access write : writeGraphList");
		try {
			for (Iterator<Integer> it = glist.keySet().iterator(); it.hasNext();) {
				int	graphid = (int) it.next();
				
				batch.put(Ints.toByteArray(graphid), glist.get(graphid));
				
			}
		
			graph_tb.write(batch);
			batch.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void writeNodeBlockList(HashMap<Long, byte[]> nodeBlockList,
			int graphid) {
		log.debug("Disk access write : writeNodeBlockList1");
		WriteBatch batch = node_tb.createWriteBatch();
		
		try {
			for (Iterator<Long> it = nodeBlockList.keySet().iterator(); it.hasNext();) {
				long nodeid = (long) it.next();
				
				byte[] rowkey = Bytes.concat(Ints.toByteArray(graphid),Longs.toByteArray(nodeid));
				
				batch.put(rowkey, nodeBlockList.get(nodeid));
			}
		
			batch.close();
			node_tb.write(batch);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void writeNodeBlockList(List<Long> nodeList, byte[] rawBlockList,
			int graphid) {
		log.debug("Disk access write : writeNodeBlockList2");
		WriteBatch batch = node_tb.createWriteBatch();
		
		try {
			for (Iterator<Long> it = nodeList.iterator(); it.hasNext();) {
				long nodeid = (long) it.next();
				
				byte[] rowkey = Bytes.concat(Ints.toByteArray(graphid),Longs.toByteArray(nodeid));
				
				batch.put(rowkey, rawBlockList);
			}
		
			node_tb.write(batch);
			batch.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void writeBlock(long blockid, byte[] bdata) {
		log.debug("Disk access write : writeBlock");
		byte[] rowkey = Longs.toByteArray(blockid);
		block_tb.put(rowkey,bdata);
	}

	@Override
	public void writeBlockList(HashMap<Long, byte[]> blist) {
		log.debug("Disk access write : writeBlockList");
		WriteBatch batch = block_tb.createWriteBatch();
		
		try {
			for (Iterator<Long> it = blist.keySet().iterator(); it.hasNext();) {
				long blockid = (long) it.next();
				
				byte[] rowkey = Longs.toByteArray(blockid);
				batch.put(rowkey, blist.get(blockid));
				
			}
			
			batch.close();
			block_tb.write(batch);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void setSystemProperties(byte[] sysprop) {
		log.debug("Disk access write : setSystemProperties");
		//put system properties into graph table
		graph_tb.put(SYSPROPID, sysprop);
	}

	@Override
	public Iterator<Long> getNodeList2(int graphid) {
		DBIterator it = node_tb.iterator();
		
		//start key is graph id (int) + node id (long)
		byte[] startRowKey = Bytes.concat(Ints.toByteArray(graphid),Longs.toByteArray(0));
		//make iterator go to the start position
		it.seek(startRowKey);
		
		LevelDBResultIteratorWrapper wrapper = new LevelDBResultIteratorWrapper(it,graphid);
		
		return wrapper;
	}
	
	private void deleteAllData(){
		//close tables
		closeDb();
		
		try {
			deleteRecursive(new File(dbDir));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private boolean deleteRecursive(File path) throws FileNotFoundException{
//        if (!path.exists()) throw new FileNotFoundException(path.getAbsolutePath());
        boolean ret = true;
        if (path.isDirectory()){
            for (File f : path.listFiles()){
                ret = ret && deleteRecursive(f);
            }
        }
        return ret && path.delete();
    }
	
	private byte[] splitBytes(byte[] byteArray, int byteLength, int offset){
		if(offset + byteLength > byteArray.length){
			return null;
		}
		byte[] newarr = new byte[byteLength];
		
		for (int i = 0; i < byteLength; i++) {
			newarr[i] = byteArray[offset + i];
		}
		
		return newarr;
	}
	
	private void openDb(){
		try {
			this.graph_tb = factory.open(new File(dbDir + "/" + GRAPH_TABLE), options);
			this.node_tb = factory.open(new File(dbDir + "/" + NODE_TABLE), options);
			this.block_tb = factory.open(new File(dbDir + "/" + BLOCK_TABLE), options);
		} catch (IOException e) {
			System.err.println("Problem opening LevelDB tables.");
			e.printStackTrace();
		}
	}

	@Override
	public void closeDb() {
		try {
			node_tb.close();
			block_tb.close();
			graph_tb.close();
		} catch (IOException e) {
			System.err.println("Problem closing LevelDB tables.");
			e.printStackTrace();
		}

	}
}

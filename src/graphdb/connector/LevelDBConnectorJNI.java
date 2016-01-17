package graphdb.connector;
//package graphdb.graph;
//
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.LinkedList;
//import java.util.List;
//import static org.fusesource.leveldbjni.JniDBFactory.*;
//
//import org.iq80.leveldb.DB;
//import org.iq80.leveldb.DBIterator;
//import org.iq80.leveldb.Options;
//import org.iq80.leveldb.WriteBatch;
//
//import com.google.common.primitives.Bytes;
//import com.google.common.primitives.Ints;
//import com.google.common.primitives.Longs;
//
//public class LevelDBConnectorJNI implements DatabaseConnector {
//	private final byte[] SYSPROPID = Ints.toByteArray(0);
//	private final byte[] GLIST_START_ROW = Ints.toByteArray(1);
//	
//	private final String GRAPH_TABLE = "graph_table";
//	private final String NODE_TABLE = "node_table";
//	private final String BLOCK_TABLE = "block_table";
//	
//	private DB graph_tb = null;
//	private DB node_tb = null;
//	private DB block_tb = null;
//	
//	private int graph_table_users = 0;
//	private int node_table_users = 0;
//	private int block_table_users = 0;
//	
//	private Options options;
//	
//	
//	public LevelDBConnectorJNI() {
//		//create options
//		options = new Options();
//		options.createIfMissing(true);
//	}
//	
//	private class LevelDBResultIteratorWrapper implements Iterator<Long> {
//		private DBIterator it;
//		int graphid;
//		DB table;
//		
//		public LevelDBResultIteratorWrapper(DBIterator it, int graphid, DB table) {
//			this.graphid = graphid;
//			this.it = it;
//			this.table = table;
//		}
//		
//		//as long as the graph is the same continue iterating
//		@Override
//		public boolean hasNext() {
//			if(it.hasNext() && (Ints.fromByteArray(splitBytes(it.peekNext().getKey(),Ints.BYTES,0)) == graphid)){
//				return true;
//			}
//			else {
//				closeTable(table);
//				return false;
//			}
//		}
//
//		@Override
//		public Long next() {
//			return Longs.fromByteArray(splitBytes(it.next().getKey(), Longs.BYTES, Ints.BYTES));
//		}
//
//		@Override
//		public void remove() {
//			it.remove();
//		}
//	}
//	
//	@Override
//	public void initGraphDb(byte[] sysprop) {
//		//delete all db table directories
//		deleteAllData();
//		
//		//initialize node and block tables
//		closeTable(openTable(NODE_TABLE));
//		closeTable(openTable(BLOCK_TABLE));
//		
//		//initialize graph table
//		DB graph_table = openTable(GRAPH_TABLE);
//		
//		//put system properties into graph table
//		graph_table.put(SYSPROPID, sysprop);
//		
//		closeTable(graph_table);
//	}
//
//	@Override
//	public byte[] getSystemProperties() {
//		//open graph table
//		DB graph_table = openTable(GRAPH_TABLE);
//		//get data from db
//		byte[] rowkey = SYSPROPID;
//		byte[] res = graph_table.get(rowkey);
//		
//		closeTable(graph_table);
//		return res;
//	}
//
//	@Override
//	public HashMap<Integer, byte[]> getGraphList() {
//		HashMap<Integer, byte[]> glist = new HashMap<Integer,byte[]>();
//		
//		DB graph_table = openTable(GRAPH_TABLE);
//		
//		DBIterator it = graph_table.iterator();
//		for(it.seek(GLIST_START_ROW); it.hasNext(); it.next()) {
//			glist.put(Ints.fromByteArray(it.peekNext().getKey()), it.peekNext().getValue());
//		}
//		closeTable(graph_table);
//		return glist;
//	}
//
//	@Override
//	public List<Long> getNodeList(int graphid) {
//		List<Long> nlist = new LinkedList<Long>();
//		
//		DB node_table = openTable(NODE_TABLE);
//		
//		DBIterator it = node_table.iterator();
//		//start key is graph id (int) + node id (long)
//		byte[] startRowKey = Bytes.concat(Ints.toByteArray(graphid),Longs.toByteArray(0));
//		
//		//as long as the graph id part doesn't change go on looping
//		for(it.seek(startRowKey); it.hasNext(); it.next()){
//			byte[] rowkey = it.peekNext().getKey();
//			if(Ints.fromByteArray(splitBytes(rowkey,Ints.BYTES,0)) != graphid){
//				break;
//			}
//			nlist.add(Longs.fromByteArray(splitBytes(rowkey, Longs.BYTES, Ints.BYTES)));
//		}
//		
//		closeTable(node_table);
//		
//		return nlist;
//	}
//
//	@Override
//	public byte[] getBlockList(long nodeid, int graphid) {
//		byte[] blist = null;
//		
//		DB node_table = openTable(NODE_TABLE);
//		byte[] rowkey = Bytes.concat(Ints.toByteArray(graphid),Longs.toByteArray(nodeid));
//		
//		blist = node_table.get(rowkey);
//		
//		closeTable(node_table);
//		
//		return blist;
//	}
//
//	@Override
//	public byte[] getBlock(long blockid) {
//		byte[] block = null;
//		
//		DB block_table = openTable(BLOCK_TABLE);
//		
//		byte[] rowkey = Longs.toByteArray(blockid);
//		block = block_table.get(rowkey);
//		
//		closeTable(block_table);
//		
//		return block;
//	}
//
//	@Override
//	public void writeGraphList(HashMap<Integer, byte[]> glist) {
//		DB graph_table = openTable(GRAPH_TABLE);
//		WriteBatch batch = graph_table.createWriteBatch();
//		
//		try {
//			for (Iterator<Integer> it = glist.keySet().iterator(); it.hasNext();) {
//				int	graphid = (int) it.next();
//				
//				batch.put(Ints.toByteArray(graphid), glist.get(graphid));
//				
//			}
//		
//			graph_table.write(batch);
//			batch.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//		closeTable(graph_table);
//	}
//
//	@Override
//	public void writeNodeBlockList(HashMap<Long, byte[]> nodeBlockList,
//			int graphid) {
//		DB node_table = openTable(NODE_TABLE);
//		WriteBatch batch = node_table.createWriteBatch();
//		
//		try {
//			for (Iterator<Long> it = nodeBlockList.keySet().iterator(); it.hasNext();) {
//				long nodeid = (long) it.next();
//				
//				byte[] rowkey = Bytes.concat(Ints.toByteArray(graphid),Longs.toByteArray(nodeid));
//				
//				batch.put(rowkey, nodeBlockList.get(nodeid));
//			}
//		
//			node_table.write(batch);
//			batch.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//		closeTable(node_table);
//		
//	}
//
//	@Override
//	public void writeNodeBlockList(List<Long> nodeList, byte[] rawBlockList,
//			int graphid) {
//		DB node_table = openTable(NODE_TABLE);
//		WriteBatch batch = node_table.createWriteBatch();
//		
//		try {
//			for (Iterator<Long> it = nodeList.iterator(); it.hasNext();) {
//				long nodeid = (long) it.next();
//				
//				byte[] rowkey = Bytes.concat(Ints.toByteArray(graphid),Longs.toByteArray(nodeid));
//				
//				batch.put(rowkey, rawBlockList);
//			}
//		
//			node_table.write(batch);
//			batch.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//		closeTable(node_table);
//		
//	}
//
//	@Override
//	public void writeBlock(long blockid, byte[] bdata) {
//		DB block_table = openTable(BLOCK_TABLE);
//		
//		byte[] rowkey = Longs.toByteArray(blockid);
//		block_table.put(rowkey,bdata);
//		
//		closeTable(block_table);
//	}
//
//	@Override
//	public void writeBlockList(HashMap<Long, byte[]> blist) {
//		DB block_table = openTable(BLOCK_TABLE);
//		WriteBatch batch = block_table.createWriteBatch();
//		
//		try {
//			for (Iterator<Long> it = blist.keySet().iterator(); it.hasNext();) {
//				long blockid = (long) it.next();
//				
//				byte[] rowkey = Longs.toByteArray(blockid);
//				batch.put(rowkey, blist.get(blockid));
//				
//			}
//			
//			block_table.write(batch);
//			batch.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//		closeTable(block_table);
//	}
//
//	@Override
//	public void setSystemProperties(byte[] sysprop) {
//		//initialize graph table
//		DB graph_table = openTable(GRAPH_TABLE);
//		
//		//put system properties into graph table
//		graph_table.put(SYSPROPID, sysprop);
//		
//		closeTable(graph_table);
//	}
//
//	@Override
//	public Iterator<Long> getNodeList2(int graphid) {
//		DB node_table = openTable(NODE_TABLE);
//		
//		DBIterator it = node_table.iterator();
//		//start key is graph id (int) + node id (long)
//		byte[] startRowKey = Bytes.concat(Ints.toByteArray(graphid),Longs.toByteArray(0));
//		//make iterator go to the start position
//		it.seek(startRowKey);
//		
//		LevelDBResultIteratorWrapper wrapper = new LevelDBResultIteratorWrapper(it,graphid,node_table);
//		
//		//closing the table will be handled in the wrapper		
//		return wrapper;
//	}
//	
//	private DB openTable(String tableName){
//		DB db = null;
//		
//		System.out.println("Openning: "+ tableName);
//		
//		try {
//			if(tableName.equals(GRAPH_TABLE)){
//				if(graph_tb != null){
//					graph_table_users++;
//					return graph_tb;
//				}
//				else {
//					db = factory.open(new File(tableName), options);
//					graph_tb = db;
//				}
//			} else if(tableName.equals(NODE_TABLE)){
//				if(node_tb != null){
//					node_table_users++;
//					return node_tb;
//				}
//				else {
//					db = factory.open(new File(tableName), options);
//					node_tb = db;
//				}
//			} else {
//				if(block_tb != null){
//					block_table_users++;
//					return block_tb;
//				}
//				else {
//					db = factory.open(new File(tableName), options);
//					block_tb = db;
//				}
//			}
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//		return db;
//	}
//	
//	private void closeTable(DB table){
//		
//		try {
//			if(table.equals(graph_tb)){
//				if(--graph_table_users > 0){
//					return;
//				}
//				else {
//					table.close();
//					graph_tb = null;
//					System.out.println("Closing: "+ table.toString());
//				}
//			} else if(table.equals(node_tb)){
//				if(--node_table_users > 0){
//					return;
//				}
//				else {
//					table.close();
//					node_tb = null;
//					System.out.println("Closing: "+ table.toString());
//				}
//			} else {
//				if(--block_table_users > 0){
//					return;
//				}
//				else {
//					table.close();
//					block_tb = null;
//					System.out.println("Closing: "+ table.toString());
//				}
//			}
//			
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
//	
//	private void deleteAllData(){
//		File graph_dir = new File(GRAPH_TABLE);
//		File node_dir = new File(NODE_TABLE);
//		File block_dir = new File(BLOCK_TABLE);
//		
//		try {
//			deleteRecursive(graph_dir);
//			deleteRecursive(node_dir);
//			deleteRecursive(block_dir);
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		}
//	}
//	
//	private boolean deleteRecursive(File path) throws FileNotFoundException{
////        if (!path.exists()) throw new FileNotFoundException(path.getAbsolutePath());
//        boolean ret = true;
//        if (path.isDirectory()){
//            for (File f : path.listFiles()){
//                ret = ret && deleteRecursive(f);
//            }
//        }
//        return ret && path.delete();
//    }
//	
//	private byte[] splitBytes(byte[] byteArray, int byteLength, int offset){
//		if(offset + byteLength > byteArray.length){
//			return null;
//		}
//		byte[] newarr = new byte[byteLength];
//		
//		for (int i = 0; i < byteLength; i++) {
//			newarr[i] = byteArray[offset + i];
//		}
//		
//		return newarr;
//	}
//}

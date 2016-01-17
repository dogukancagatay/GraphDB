package graphdb.connector;

import graphdb.graph.DatabaseConnector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseConnector implements DatabaseConnector {
	private Configuration conf;
	
	private final byte[] SYSPROPID = Bytes.toBytes(0);
	
	private final byte[] COL_FAMILY	= Bytes.toBytes("data");
	private final byte[] COL_QUALIFIER = Bytes.toBytes("data");
	
	private final byte[] GRAPH_TABLE = Bytes.toBytes("graph_index");	// sysprops reside here
	private final byte[] NODE_TABLE = Bytes.toBytes("node_index");		// graphid+nodeid -> block ids
	private final byte[] BLOCK_TABLE = Bytes.toBytes("block_index");	// blockid -> block data
	
	private final byte[] GLIST_START_ROW = Bytes.toBytes(1);
	
	private HTable graph_tb = null;
	private HTable node_tb = null;
	private HTable block_tb = null;
	
	private class HBaseNodeResultIteratorWrapper implements Iterator<Long> {
		private Iterator<Result> resit;

		public HBaseNodeResultIteratorWrapper(Iterator<Result> resit) {
			this.resit = resit;
		}

		@Override
		public boolean hasNext() {
			return resit.hasNext();
		}

		@Override
		public Long next() {
			return Bytes.toLong(resit.next().getRow());
		}

		@Override
		public void remove() {
			resit.remove();
		}
		
	}
	
	public HBaseConnector(HashMap<String,String> configuration){
		//create hbase configuration
		this.conf = HBaseConfiguration.create();
		
		//set configurations from BufferManager
		for (Iterator<String> it = configuration.keySet().iterator(); it.hasNext();) {
			String key = it.next();
			
			this.conf.set(key,configuration.get(key));			
		}
		
		//initialize graph, node, block tables
		openDb();
	}

	public byte[] getSystemProperties(){
		try {
			Get getid = new Get(SYSPROPID);
			getid.addColumn(COL_FAMILY, COL_QUALIFIER);
			
			Result r = graph_tb.get(getid);
		
			if(!r.isEmpty()){
				return r.getValue(COL_FAMILY, COL_QUALIFIER);
			}
			
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	@Override
	public HashMap<Integer,byte[]> getGraphList(){
		HashMap<Integer, byte[]> glist = new HashMap<Integer, byte[]>();
		
		try {
			Scan scan = new Scan();
			scan.setStartRow(GLIST_START_ROW);
			scan.addColumn(COL_FAMILY, COL_QUALIFIER);
			
			ResultScanner rs = graph_tb.getScanner(scan);
			
			for (Result r : rs) {
				glist.put(Bytes.toInt(r.getRow()), r.getValue(COL_FAMILY, COL_QUALIFIER));
			}
			
			rs.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return glist;
	}
	
	@Deprecated
	@Override
	public List<Long> getNodeList(int graphid) {
		List<Long> nlist = new LinkedList<Long>();
		
		try {
			Scan scan = new Scan();
			scan.setStartRow(Bytes.add(Bytes.toBytes(graphid), Bytes.toBytes(0)));
			scan.setStopRow(Bytes.add(Bytes.toBytes(graphid + 1), Bytes.toBytes(0)));
			
			scan.addColumn(COL_FAMILY, COL_QUALIFIER);
			ResultScanner res = node_tb.getScanner(scan);
			
			
			for(Result r : res){
				//get rowkey
				byte[] rowkey = r.getRow();
				
				//offset the graph id which is integer
				long nodeid = Bytes.toLong(rowkey, Bytes.SIZEOF_INT);
				
				//add nodeid to the list
				nlist.add(nodeid);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return nlist;
	}
	
	@Override
	public Iterator<Long> getNodeList2(int graphid) {
		HBaseNodeResultIteratorWrapper wrapper = null;
		
		try {
			Scan scan = new Scan();
			scan.setStartRow(Bytes.add(Bytes.toBytes(graphid), Bytes.toBytes(0)));
			scan.setStopRow(Bytes.add(Bytes.toBytes(graphid + 1), Bytes.toBytes(0)));
			
			scan.addColumn(COL_FAMILY, COL_QUALIFIER);
			ResultScanner res = node_tb.getScanner(scan);
			
			wrapper = new HBaseNodeResultIteratorWrapper(res.iterator());
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return wrapper;
	}
	
	@Override
	public byte[] getBlockList(long nodeid, int graphid){
		byte[] blist = null;
		
		try {
			byte[] rowkey = Bytes.add(Bytes.toBytes(graphid), Bytes.toBytes(nodeid));
			
			Get get = new Get(rowkey);
			get.addColumn(COL_FAMILY, COL_QUALIFIER);
			
			Result r = node_tb.get(get);
			
			if(!r.isEmpty()){
				blist = r.getValue(COL_FAMILY, COL_QUALIFIER);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return blist;
	}
	
	@Override
	public byte[] getBlock(long blockid){
		byte[] block = null;
		
		try {
			byte[] rowkey = Bytes.toBytes(blockid);
			
			Get get = new Get(rowkey);
			get.addColumn(COL_FAMILY, COL_QUALIFIER);
			
			Result r = block_tb.get(get);
			
			if(!r.isEmpty()){
				block = r.getValue(COL_FAMILY, COL_QUALIFIER);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return block;
	}
	
	@Override
	public void writeGraphList(HashMap<Integer,byte[]> glist){
		try {
			LinkedList<Put> puts = new LinkedList<Put>();
			
			
			for (Iterator<Integer> it = glist.keySet().iterator(); it.hasNext();) {
				int key = it.next();
				
				if(key == 0){
					//skip something is wrong
					continue;
				}
				
				byte[] rowkey = Bytes.toBytes(key);
				Put put = new Put(rowkey);
				put.add(COL_FAMILY, COL_QUALIFIER, glist.get(key));
				puts.add(put);
			}
			
			graph_tb.put(puts);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void writeBlock(long blockid, byte[] blockdata){
		try {
				
			byte[] rowkey = Bytes.toBytes(blockid);
			Put put = new Put(rowkey);
			put.add(COL_FAMILY, COL_QUALIFIER, blockdata);
			
			block_tb.put(put);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void writeBlockList(HashMap<Long, byte[]> blist){
		try {
			LinkedList<Put> puts = new LinkedList<Put>();
			
			for (Iterator<Long> it = blist.keySet().iterator(); it.hasNext();) {
				long key = it.next();
				
				if(key == 0){
					//skip something is wrong
					continue;
				}
				
				byte[] rowkey = Bytes.toBytes(key);
				Put put = new Put(rowkey);
				put.add(COL_FAMILY, COL_QUALIFIER, blist.get(key));
				puts.add(put);
			}
			
			block_tb.put(puts);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void initGraphDb(byte[] sysprop) {		
		try {
			//first delete all data in the database
			deleteAllData();
			
			HBaseAdmin hadmin = new HBaseAdmin(conf);
	
		
			//the same cf for all table
			HColumnDescriptor familycol = new HColumnDescriptor("data");
			
			//create graph_index table
			HTableDescriptor graphTableDesc = new HTableDescriptor(GRAPH_TABLE);
			graphTableDesc.addFamily(familycol);
			hadmin.createTable(graphTableDesc);
			
			//create node_index table
			HTableDescriptor nodeTableDesc = new HTableDescriptor(NODE_TABLE);
			nodeTableDesc.addFamily(familycol);
			hadmin.createTable(nodeTableDesc);
			
			//create block_index table
			HTableDescriptor blockTableDesc = new HTableDescriptor(BLOCK_TABLE);
			blockTableDesc.addFamily(familycol);
			hadmin.createTable(blockTableDesc);
			
			hadmin.close();
			
			//open all tables
			openDb();
			
			// insert system property
			setSystemProperties(sysprop);
			

		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void deleteAllData() throws IOException, MasterNotRunningException, ZooKeeperConnectionException {
		HBaseAdmin hadmin = new HBaseAdmin(conf);
		//close all tables
		closeDb();
		
		//disable all tables used
		try {
			hadmin.disableTable(GRAPH_TABLE);				
			hadmin.deleteTable(GRAPH_TABLE);
		} catch(TableNotFoundException e){
			System.err.println("graph_table didn't exist in the database.");
		}
		
		try {
			hadmin.disableTable(NODE_TABLE);
			hadmin.deleteTable(NODE_TABLE);
		} catch(TableNotFoundException e){
			System.err.println("node_table didn't exist in the database.");
		}
		
		try {
			hadmin.disableTable(BLOCK_TABLE);
			hadmin.deleteTable(BLOCK_TABLE);
		} catch(TableNotFoundException e){
			System.err.println("block_table didn't exist in the database.");
		}
		
		hadmin.close();
	}

	@Override
	public void setSystemProperties(byte[] sysprop) {
		try {
			byte[] rowkey = SYSPROPID;
			Put put = new Put(rowkey);
			put.add(COL_FAMILY, COL_QUALIFIER, sysprop);
			
			graph_tb.put(put);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void writeNodeBlockList(HashMap<Long, byte[]> nodeBlockList, int graphid) {
		try {
			LinkedList<Put> puts = new LinkedList<Put>();
			
			for (Iterator<Long> it = nodeBlockList.keySet().iterator(); it.hasNext();) {
				long nodeid = it.next();
				
				
				byte[] rowkey = Bytes.add(Bytes.toBytes(graphid), Bytes.toBytes(nodeid));
				Put put = new Put(rowkey);
				put.add(COL_FAMILY, COL_QUALIFIER, nodeBlockList.get(nodeid));
				puts.add(put);
			}
			
			node_tb.put(puts);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void writeNodeBlockList(List<Long> nodeList, byte[] rawBlockList,
			int graphid) {
		try {
			LinkedList<Put> puts = new LinkedList<Put>();
			
			for (Iterator<Long> it = nodeList.iterator(); it.hasNext();) {
				long nodeid = it.next();
				
				
				byte[] rowkey = Bytes.add(Bytes.toBytes(graphid), Bytes.toBytes(nodeid));
				Put put = new Put(rowkey);
				put.add(COL_FAMILY, COL_QUALIFIER, rawBlockList);
				puts.add(put);
			}
			
			node_tb.put(puts);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	private void openDb(){
		try {
			graph_tb = new HTable(this.conf, GRAPH_TABLE);
			node_tb = new HTable(this.conf, NODE_TABLE);
			block_tb = new HTable(this.conf, BLOCK_TABLE);
		} catch (IOException e) {
			System.err.println("Problem opening HBase tables.");
			e.printStackTrace();
		}
	}

	@Override
	public void closeDb() {
		try {
			graph_tb.close();
			node_tb.close();
			block_tb.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


}

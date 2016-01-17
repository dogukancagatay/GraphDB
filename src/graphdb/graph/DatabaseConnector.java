package graphdb.graph;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public interface DatabaseConnector {
	public void initGraphDb(byte[] sysprop);
	public byte[] getSystemProperties();
	public void setSystemProperties(byte[] sysprop);
	
	public HashMap<Integer,byte[]> getGraphList();
	public List<Long> getNodeList(int graphid);
	public Iterator<Long> getNodeList2(int graphid);
	
	public byte[] getBlockList(long nodeid, int graphid);
	public byte[] getBlock(long pageid);
	
	public void writeGraphList(HashMap<Integer,byte[]> glist);
	public void writeNodeBlockList(HashMap<Long, byte[]> nodeBlockList, int graphid);
	public void writeNodeBlockList(List<Long> nodeList, byte[] rawBlockList, int graphid);
	public void writeBlock(long blockid, byte[] bdata);
	public void writeBlockList(HashMap<Long, byte[]> blist);
	
	public void closeDb();
}

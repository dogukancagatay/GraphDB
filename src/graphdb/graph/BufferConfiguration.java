package graphdb.graph;

public class BufferConfiguration {
	private int maxBufferSize; // number of blocks in buffer
	private int maxBlockSize; // block size in bytes 

	public BufferConfiguration() {
		maxBlockSize = 8*1024*1024; // default size is 80 MB
		maxBufferSize = 100; // default is 10 blocks in the buffer (800 MB)
	}

	/**
	 * In the constructor you would specify how many blocks will be held in the buffer
	 * and individual block size. 
	 * 
	 * The total space needed for holding blocks in the buffer can be found by 
	 * 
	 * 		maxBufferSize x maxBlockSize.
	 * 
	 * This multiplication will not give you the exact memory consumption but it should
	 * give you an idea of space needed in memory. The exact space needed in the memory
	 * would be higher.
	 * 
	 * @param maxBufferSize The number of blocks in the buffer.
	 * @param maxBlockSize Individual block size in bytes.
	 */
	public BufferConfiguration(int maxBufferSize, int maxBlockSize) {
		super();
		this.maxBufferSize = maxBufferSize;
		this.maxBlockSize = maxBlockSize;
	}

	/**
	 * 
	 * @return The number of blocks in the buffer. 
	 */
	public int getMaxBufferSize() {
		return maxBufferSize;
	}

	/**
	 * 
	 * @param maxBufferItems The number of blocks in the buffer. (Default: 100 blocks)
	 */
	public void setMaxBufferSize(int maxBufferSize) {
		this.maxBufferSize = maxBufferSize;
	}

	/**
	 * 
	 * @return Individual block size in bytes.
	 */
	public int getMaxBlockSize() {
		return maxBlockSize;
	}

	/**
	 * 
	 * @param maxBlockSize Individual block size in bytes. (Default: 80 MB)
	 */
	public void setMaxBlockSize(int maxBlockSize) {
		this.maxBlockSize = maxBlockSize;
	}
}

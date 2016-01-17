package graphdb.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;

public class SequenceByteArrayOutputStream extends OutputStream {
    private LinkedList<byte[]> baList;
    private ByteArrayOutputStream out;

    private int capacity;
	private int blockSize;

    public SequenceByteArrayOutputStream(int blockSize) {
        this.baList = new LinkedList<byte[]>();
        this.blockSize = blockSize;

    	//create the first byte[] stream
        this.out = new ByteArrayOutputStream();
        this.capacity = blockSize;

    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        while(len>0) {
            int sz = (int)Math.min(len, capacity);
            out.write(b,off,sz);
            capacity -=sz;
            len-=sz;
            off+=sz;
            swapIfNeeded();
        }
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
        capacity--;
        swapIfNeeded();
    }

    @Override
    public void write(byte[] b) throws IOException {
    	write(b, 0, b.length);
    }

    private void swapIfNeeded() throws IOException {
        if(capacity >0) return;
        out.close();
        out=next(out);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
    	if(out != null){
            out.close();
            
            //if the last byte array is dirty
            if(capacity != blockSize){
//                baList.appendElement(out.toByteArray());
                baList.add(out.toByteArray());
            }
    	}

        out=null;
    }

    /**
     * Fetches the next {@link OutputStream} to write to,
     * along with their capacity.
     */
     private ByteArrayOutputStream next(ByteArrayOutputStream current) throws IOException{
    	 //add current block's byte[] to balist
//    	 baList.appendElement(current.toByteArray());
    	 baList.add(current.toByteArray());

    	 //create an empty block
    	 ByteArrayOutputStream newStream = new ByteArrayOutputStream();
    	 capacity = blockSize;

    	 //remove current block
    	 current = null;

    	 return newStream;
     }
     
     public LinkedList<byte[]> toByteArrayList(){
    	 return baList;
     }
}

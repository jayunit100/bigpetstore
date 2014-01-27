package org.bigtop.bigpetstore.generator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bigtop.bigpetstore.generator.TransactionIteratorFactory.STATE;

/**
 * What does an `InputSplit` actually do?
 * From the Javadocs, it looks like ... absolutely nothing.
 *
 * Note: for some reason, you *have* to implement Writable,
 * even if your methods do nothing, or you will got strange
 * and un-debuggable null pointer exceptions.
 */
public class PetStoreTransactionInputSplit extends InputSplit implements Writable {


	public PetStoreTransactionInputSplit() {
	}

	public int records; public STATE state;
	public PetStoreTransactionInputSplit(int records,STATE state) {
        this.records=records;
        this.state=state;
    }
	
	public void readFields(DataInput arg0) throws IOException {
	    records=arg0.readInt();
        state=STATE.valueOf(arg0.readUTF());
	}

	public void write(DataOutput arg0) throws IOException {
        arg0.writeInt(records);
        arg0.writeUTF(state.name());
	}
	
	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[] {};
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return 100;
	}
}

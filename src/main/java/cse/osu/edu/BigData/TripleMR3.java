package cse.osu.edu.BigData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class TripleMR3 implements Writable{
	LongWritable v1;
	LongWritable v2;
	LongWritable v3;
	
	public TripleMR3() {
		this.v1 = new LongWritable();
		this.v2 = new LongWritable();
		this.v3 = new LongWritable();
	}
	
	public TripleMR3(LongWritable v1, LongWritable v2, LongWritable v3) {
		this.v1=v1;
		this.v2=v2;
		this.v3=v3;
	}
	public void readFields(DataInput in) throws IOException {
		this.v1.readFields(in);
		this.v2.readFields(in);
		this.v3.readFields(in);
	}
	public void write(DataOutput out) throws IOException {
		this.v1.write(out);
		this.v2.write(out);
		this.v3.write(out);
	}
	public LongWritable getV1() {
		return v1;
	}
	public LongWritable getV2() {
		return v2;
	}
	public LongWritable getV3() {
		return v3;
	}
	
	public void setV1(LongWritable v1) {
		this.v1 = v1;
	}
	public void setV2(LongWritable v2) {
		this.v2 = v2;
	}
	public void setV3(LongWritable v3) {
		this.v3 = v3;
	}
	@Override
	public String toString() {
		return v1.toString()+" "+v2.toString()+" "+v3.toString();
	}

}

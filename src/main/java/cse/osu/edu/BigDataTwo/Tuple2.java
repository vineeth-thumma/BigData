package cse.osu.edu.BigDataTwo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class Tuple2 implements WritableComparable<Tuple2>{
	LongWritable v1;
	FloatWritable v2;
	
	public Tuple2() {
		this.v1 = new LongWritable();
		this.v2 = new FloatWritable();
	}
	
	public Tuple2(LongWritable v1, FloatWritable v2) {
		this.v1=v1;
		this.v2=v2;
	}
	public void readFields(DataInput in) throws IOException {
		this.v1.readFields(in);
		this.v2.readFields(in);
	}
	public void write(DataOutput out) throws IOException {
		this.v1.write(out);
		this.v2.write(out);
	}
	public LongWritable getV1() {
		return v1;
	}
	public FloatWritable getV2() {
		return v2;
	}
	
	public void setV1(LongWritable v1) {
		this.v1 = v1;
	}
	public void setV2(FloatWritable v2) {
		this.v2 = v2;
	}
	@Override
	public String toString() {
		return v1.toString()+" "+v2.toString();
	}

	public int compareTo(Tuple2 o) {
		if(this.v1.get() == o.v1.get()) {
			return (v2.get()-o.v2.get())<0 ? -1:1;
		}
		return (v1.get()-o.v1.get())<0 ? -1:1;
	}

	
}


package cse.osu.edu.BigDataTwo;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

public class MR1Comparator implements RawComparator<Tuple2>{

	public int compare(Tuple2 o1, Tuple2 o2) {
		return o1.getV1().get()<o2.getV1().get() ? -1 : (o1.getV1().get()==o2.getV1().get()) ? 0 : 1;
	}

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		long first1 = WritableComparator.readLong(b1, s1);
		long first2 = WritableComparator.readLong(b2, s2);
		
		return first1 < first2 ? -1 : first1 == first2 ? 0 : 1;
	}
}

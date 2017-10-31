package cse.osu.edu.BigData;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class MR3Partitioner implements Partitioner<TupleMR2MapOutput, TripleMR3> {

	public void configure(JobConf arg0) {
				
	}

	public int getPartition(TupleMR2MapOutput key, TripleMR3 value, int numberReduceTasks) {
		if(numberReduceTasks==0) {
			return 0;
		}
		return (int)key.v1.get();
	}
}

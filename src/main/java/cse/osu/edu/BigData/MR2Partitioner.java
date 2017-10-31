package cse.osu.edu.BigData;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class MR2Partitioner implements Partitioner<TupleMR2MapOutput, TupleMR2MapOutput> {

	public void configure(JobConf arg0) {
				
	}

	public int getPartition(TupleMR2MapOutput key, TupleMR2MapOutput value, int numberReduceTasks) {
		if(numberReduceTasks==0) {
			return 0;
		}
		return (int)key.v1.get();
	}
}

package cse.osu.edu.BigDataTwo;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class MR1Partitioner implements Partitioner<Tuple2, Tuple2> {

	public void configure(JobConf arg0) {
				
	}

	public int getPartition(Tuple2 key, Tuple2 value, int numberReduceTasks) {
		if(numberReduceTasks==0) {
			return 0;
		}
		return (int)(key.v1.get()%numberReduceTasks);
	}
}

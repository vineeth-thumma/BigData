package cse.osu.edu.BigData;
import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException; 

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.*;
import cse.osu.edu.BigData.TupleMR2MapOutput;
	
public class MapReducePhaseTwo {
	
	static int numberOfReducers=10;
		
	   //Mapper class for MR-2
		public static class MR2_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, TupleMR2MapOutput, TupleMR2MapOutput>
		{
			//Map function
			public void map(LongWritable key, Text value, OutputCollector<TupleMR2MapOutput, TupleMR2MapOutput> output, Reporter reporter) throws IOException
			{
				String[] values = value.toString().trim().split("\\s+");
				long reducerNumber = Long.parseLong(values[0])/(3000001);
				output.collect(new TupleMR2MapOutput(new LongWritable(reducerNumber), new LongWritable(Long.parseLong(values[0]))) , 
						new TupleMR2MapOutput(new LongWritable(Long.parseLong(values[0])), new LongWritable(Long.parseLong(values[1]))));
			}
		}
		
		//Reducer class for MR-2
		public static class MR2_Reducer extends MapReduceBase implements Reducer<TupleMR2MapOutput, TupleMR2MapOutput, TupleMR2MapOutput, TupleMR2MapOutput>
		{
			MultipleOutputs prefixSumOutput=null;
			
			public void configure(JobConf job ) {
				prefixSumOutput = new MultipleOutputs(job);
			}
			
			//Reduce function
			@SuppressWarnings("unchecked")
			public void reduce(TupleMR2MapOutput key, Iterator <TupleMR2MapOutput> value, OutputCollector<TupleMR2MapOutput, TupleMR2MapOutput> output, Reporter reporter) throws IOException
			{
					long cumulativeRank=1;
					long tmpValue=0;
					long count=0;
					while(value.hasNext()) 
					{	
						TupleMR2MapOutput currentTuple=value.next();
						tmpValue = currentTuple.getV2().get();
						key.setV2(new LongWritable(cumulativeRank));
						cumulativeRank += tmpValue;
						count++;
						//output.collect(key.getV1(), currentTuple);
						output.collect(key, currentTuple);
					}
					prefixSumOutput.getCollector("prefixsum" , reporter).collect(key.getV1(), 
								new TupleMR2MapOutput(new LongWritable(count), new LongWritable(cumulativeRank)));
			}
			
			@Override
			public void close() throws IOException {
				prefixSumOutput.close();
			}
		}
		
		public static void main(String args[])throws Exception 
		{ 
		   JobConf conf = new JobConf(MapReducePhaseTwo.class); 
		   
		   conf.setJobName("NumberRanking-MR2"); 
		   conf.setOutputKeyClass(TupleMR2MapOutput.class);
		   conf.setOutputValueClass(TupleMR2MapOutput.class);
		   conf.setMapperClass(MR2_Mapper.class); 
		   conf.setReducerClass(MR2_Reducer.class);
		   conf.setNumReduceTasks(numberOfReducers);
		   conf.setPartitionerClass(MR2Partitioner.class);
		   conf.setOutputValueGroupingComparator(MR2Comparator.class);
		   conf.setInputFormat(TextInputFormat.class); 
		   conf.setOutputFormat(TextOutputFormat.class);
		   		      
		   FileInputFormat.setInputPaths(conf, new Path(args[0])); 
		   FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
		   MultipleOutputs.addNamedOutput(conf, "prefixsum", TextOutputFormat.class, Text.class, Text.class);
		   		     
		   JobClient.runJob(conf); 
		} 
}

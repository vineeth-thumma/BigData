package cse.osu.edu.BigData;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import cse.osu.edu.BigData.TupleMR2MapOutput;
import cse.osu.edu.BigData.TripleMR3;



public class MapReducePhaseThree {
			static Path pathToPrefixSum;
			static int numberOfReducers=10;
			static long[] count = new long[numberOfReducers+1];
			static long[] cumulativeRank = new long[numberOfReducers+1]; 
			
			//Mapper class for MR-3
			public static class MR3_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, TupleMR2MapOutput, TripleMR3>
			{
				URI[] cacheFiles;// = new URI[1];
				BufferedReader cacheReader;
		
				public void configure(JobConf conf) {
					//
					try{
						cacheFiles = DistributedCache.getCacheFiles(conf);
						FileSystem fs = FileSystem.get(cacheFiles[0], conf);
						FileStatus[] fileStatus = fs.listStatus(pathToPrefixSum);
						for(FileStatus status: fileStatus) {
							cacheReader = new BufferedReader(new FileReader(status.toString()));
							String line;
							if((line=cacheReader.readLine()) != null) {
								String[] words = line.toString().trim().split("\\s+");
								count[Integer.parseInt(words[0])+1] = Long.parseLong(words[1]);
								cumulativeRank[Integer.parseInt(words[0])+1] = Long.parseLong(words[2]);
								
							}
						}
						for(int i=1; i<=numberOfReducers; i++) {
							count[i] += count[i-1];
							cumulativeRank[i] += cumulativeRank[i-1];
						}
						
					}
					catch(IOException e) {
						e.printStackTrace();
					}
				}
				
				//Map function
				public void map(LongWritable key, Text value, OutputCollector<TupleMR2MapOutput, TripleMR3> output, Reporter reporter) throws IOException
				{
					String[] values = value.toString().trim().split("\\s+");
					
					output.collect(new TupleMR2MapOutput(new LongWritable(Long.parseLong(values[0])), new LongWritable(Long.parseLong(values[2]))) , 
							new TripleMR3(new LongWritable(Long.parseLong(values[2])),new LongWritable(Long.parseLong(values[1])), new LongWritable(Long.parseLong(values[3]))));
					
				}
									
			}
			
			//Reducer class for MR-3
			public static class MR3_Reducer extends MapReduceBase implements Reducer<TupleMR2MapOutput, TripleMR3, LongWritable, TripleMR3>
			{
								
				//Reduce function
				@SuppressWarnings("unchecked")
				public void reduce(TupleMR2MapOutput key, Iterator <TripleMR3> value, OutputCollector<LongWritable, TripleMR3> output, Reporter reporter) throws IOException
				{
						int reducerNumber = (int)key.getV1().get();
						long lineNumber= cumulativeRank[reducerNumber]+1;
						long denseRank = count[reducerNumber]+1;
						while(value.hasNext()) 
						{	
							
							TripleMR3 currentTriple=value.next();
							long number= currentTriple.getV1().get();
							long rank = currentTriple.getV2().get()+cumulativeRank[reducerNumber];
							long frequency = currentTriple.getV3().get();
							
							for(long i=0; i<frequency; i++) {
								output.collect(new LongWritable(number),
										new TripleMR3(new LongWritable(lineNumber), new LongWritable(rank), new LongWritable(denseRank)));
								lineNumber++;
							}
							denseRank++;
														
						}
						
				}
			}
			
		
			@SuppressWarnings("deprecation")
			public static void main(String args[])throws Exception 
			{ 
			   JobConf conf = new JobConf(MapReducePhaseThree.class); 
			   
			   conf.setJobName("NumberRanking-MR3"); 
			   conf.setOutputKeyClass(TupleMR2MapOutput.class);
			   conf.setOutputValueClass(TripleMR3.class);
			   conf.setMapperClass(MR3_Mapper.class);
			   conf.setReducerClass(MR3_Reducer.class);
			   conf.setNumReduceTasks(10);
			   conf.setInputFormat(TextInputFormat.class); 
			   conf.setOutputFormat(TextOutputFormat.class);
			   conf.setPartitionerClass(MR3Partitioner.class);
			   conf.setOutputValueGroupingComparator(MR2Comparator.class);
			   		      
			   DistributedCache.addCacheFile(new Path(args[2]).toUri(), conf);
			   pathToPrefixSum = new Path(args[2]);
			   FileInputFormat.setInputPaths(conf, new Path(args[0])); 
			   FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
			   
			   	 
			   JobClient.runJob(conf); 
			} 
}
